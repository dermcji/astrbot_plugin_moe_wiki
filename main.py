"""萌百问答：群友问「你认不认识 XXX」，去萌娘百科查一条回来。

答案不是模型瞎编的——先把条目开头抓下来，再按当前人设压成一句人话，末尾附链接。
查不到就不说话：自动模式下宁可漏答，也不乱编、不抢正常聊天的话。
"""

from __future__ import annotations

import asyncio
import re
import time
from urllib.parse import quote

import aiohttp

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

PLUGIN_NAME = "astrbot_plugin_moe_wiki"
PLUGIN_VER = "1.0.0"

API = "https://zh.moegirl.org.cn/api.php"
SITE = "https://zh.moegirl.org.cn/"
UA = (
    "Mozilla/5.0 (Linux; Android 14; Mobile) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"
)

# ---------------------------------------------------------------- 问句识别

# 「你认不认识 X」「你知不知道 X 是什么」「你了解 X 吗」
_PRE = (
    r"(?:(?:你|您|请问|问一下|问下|我想问|打听一下)(?:到底|究竟)?"
    r"(?:认不认识|认识|知不知道|知道|了不了解|了解|听没听说过|听说过|有没有听过|听过))"
)
# 实体后面可能跟的解释性尾巴
_TAIL = (
    r"(?:到底|究竟)?(?:是什么东西|是什么|是谁|是啥|是哪个|是哪位|什么来头|什么东西|"
    r"哪部作品|哪个角色|什么作品|什么梗|什么动漫|什么游戏|什么软件|什么动画|什么番)?"
)
_ASK_RE = re.compile(
    r"^\s*(?:@\S+\s*)?" + _PRE + r"\s*(?P<ent>.{1,24}?)\s*" + _TAIL
    + r"\s*(?:吗|么|嘛|呢|啊|呀)?\s*[?？!！。~～…]*\s*$"
)
# 「X 是什么」「X 是谁」——没有前缀的直接定义句
_DEF_RE = re.compile(
    r"^\s*(?:@\S+\s*)?(?P<ent>.{1,24}?)\s*(?:到底|究竟)?"
    r"(?:是什么东西|是什么|是谁|是啥|是哪个|是哪位|什么来头)"
    r"\s*(?:啊|呢|呀)?\s*[?？!！。~～…]*\s*$"
)

# 以这些开头的多半不是词条名，是代词或疑问句残渣
_BAD_HEAD = ("我", "你", "您", "他", "她", "它", "这", "那", "咱", "大家", "谁", "什么",
             "怎么", "为何", "为什", "如何", "哪", "几", "为")
_BAD_WORDS = ("怎么", "为什么", "如何", "什么", "干嘛", "干啥", "在哪", "多少", "是不是")


def _clean_entity(raw: str) -> str | None:
    """把捕获到的片段收拾成能拿去查的词条名，不像词条就返回 None。"""
    s = (raw or "").strip()
    for opener in ("我想问一下", "帮我查一下", "打听一下", "问一下", "我想问", "请问", "问下", "帮我查查"):
        if s.startswith(opener):
            s = s[len(opener):].strip()
            break
    s = s.strip("\"'“”‘’「」『』《》【】()（）[] \t\r\n，,。.？?！!：:；;~～…、")
    if not (1 <= len(s) <= 24):
        return None
    if s.startswith(_BAD_HEAD):
        return None
    if any(w in s for w in _BAD_WORDS):
        return None
    if re.fullmatch(r"[\d\s.\-_/]+", s):
        return None
    if not re.search(r"[\u4e00-\u9fffA-Za-z]", s):
        return None
    return s


def extract_entity(text: str) -> str | None:
    """从一句话里抠出要查的词条名；抠不出或像闲话就返回 None。"""
    msg = (text or "").strip()
    if not msg or len(msg) > 60:
        return None
    for pat in (_ASK_RE, _DEF_RE):
        m = pat.match(msg)
        if m:
            ent = _clean_entity(m.group("ent"))
            if ent:
                return ent
    return None


# ---------------------------------------------------------------- 萌百查询


class MoeWiki:
    """萌娘百科 MediaWiki API 的薄封装，带限流和近似标题兜底。"""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": UA},
                timeout=aiohttp.ClientTimeout(total=15),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _json(self, params: dict):
        # 两次请求之间留点间隔，别把萌百惹毛了
        async with self._lock:
            gap = time.monotonic() - self._last
            if gap < 0.5:
                await asyncio.sleep(0.5 - gap)
            self._last = time.monotonic()
        p = {**params, "format": "json", "formatversion": "2"}
        try:
            sess = await self._sess()
            async with sess.get(API, params=p) as r:
                if r.status != 200:
                    logger.warning(f"[moe_wiki] HTTP {r.status}")
                    return None
                return await r.json(content_type=None)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[moe_wiki] 请求失败: {e}")
            return None

    async def _by_title(self, title: str) -> dict | None:
        data = await self._json({
            "action": "query",
            "prop": "extracts",
            "exintro": 1,
            "explaintext": 1,
            "redirects": 1,
            "titles": title,
        })
        pages = ((data or {}).get("query") or {}).get("pages") or []
        if not pages:
            return None
        page = pages[0]
        if page.get("missing"):
            return None
        text = (page.get("extract") or "").strip()
        if not text:
            return None
        return {"title": page.get("title") or title, "extract": text}

    async def search(self, keyword: str) -> dict | None:
        """先按原词查；查不到就用 opensearch 找最接近的几个标题再试。"""
        hit = await self._by_title(keyword)
        if hit:
            return hit
        data = await self._json({"action": "opensearch", "search": keyword, "limit": 6})
        if not isinstance(data, list) or len(data) < 2:
            return None
        titles = [t for t in (data[1] or []) if t and t != keyword]
        for title in titles[:3]:
            hit = await self._by_title(title)
            if hit:
                hit["fuzzy"] = True
                return hit
        return None


# ---------------------------------------------------------------- 插件本体


@register(PLUGIN_NAME, "dsh", "萌百问答：问「你认不认识 X」时查萌娘百科并回一句", PLUGIN_VER)
class MoeWikiPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.config = config or {}
        self._wiki = MoeWiki()
        self._cache: dict[str, tuple[float, dict]] = {}
        self._recent: dict[str, float] = {}
        # 配置在实例化时读一次，改完配置重载插件即可生效
        self.enabled = bool(self._cfg("moe_enabled", True))
        self.auto_detect = bool(self._cfg("moe_auto_detect", True))
        self.require_at = bool(self._cfg("moe_require_at", False))
        self.use_llm = bool(self._cfg("moe_use_llm", True))
        self.with_link = bool(self._cfg("moe_with_link", True))
        try:
            self.max_chars = max(40, min(600, int(self._cfg("moe_max_chars", 160) or 160)))
        except (TypeError, ValueError):
            self.max_chars = 160
        self.allowed_qq = self._id_list("moe_allowed_qq")
        self.allowed_groups = self._id_list("moe_allowed_groups")

    # ---------------- 配置

    def _cfg(self, key: str, default=None):
        if isinstance(self.config, dict):
            return self.config.get(key, default)
        try:
            return self.config[key]
        except Exception:  # noqa: BLE001
            return default

    def _id_list(self, key: str) -> set[str]:
        raw = self._cfg(key, []) or []
        if isinstance(raw, str):
            raw = re.split(r"[,，\s]+", raw)
        return {str(x).strip() for x in raw if str(x).strip()}

    def _allowed(self, event: AstrMessageEvent) -> bool:
        """白名单门禁：两个列表都空 = 谁都能用；填了就只有列表里的能用。"""
        qq = str(event.get_sender_id() or "")
        allow = self.allowed_qq
        if allow and qq not in allow:
            logger.info(f"[moe_wiki] 拦下 {qq}：不在 moe_allowed_qq 里")
            return False
        groups = self.allowed_groups
        gid = str(event.get_group_id() or "")
        if groups and gid and gid not in groups:
            logger.info(f"[moe_wiki] 拦下群 {gid}：不在 moe_allowed_groups 里")
            return False
        return True

    def _at_me(self, event: AstrMessageEvent) -> bool:
        try:
            return bool(event.is_at_or_wake_command)
        except Exception:  # noqa: BLE001
            return False

    def _umo(self, event: AstrMessageEvent) -> str:
        return str(getattr(event, "unified_msg_origin", "") or "")

    async def _persona_prompt(self, umo: str) -> str:
        """取当前会话的人设提示词。取不到再谈「自然一点」的兜底。"""
        pm = getattr(self.context, "persona_manager", None)
        if pm is None:
            return ""
        try:
            persona = await pm.get_default_persona_v3(umo)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[moe_wiki] 读取人设失败: {e}")
            return ""
        if isinstance(persona, dict):
            return str(persona.get("prompt") or "")
        return str(getattr(persona, "prompt", "") or "")

    # ---------------- 查询与渲染

    async def _lookup(self, keyword: str) -> dict | None:
        now = time.monotonic()
        ttl = 1800.0
        cached = self._cache.get(keyword)
        if cached and now - cached[0] < ttl:
            return cached[1]
        hit = await self._wiki.search(keyword)
        if hit:
            if len(self._cache) > 200:
                self._cache.clear()
            self._cache[keyword] = (now, hit)
        return hit

    def _cooling(self, key: str) -> bool:
        now = time.monotonic()
        last = self._recent.get(key, 0.0)
        if now - last < 20:
            return True
        self._recent[key] = now
        if len(self._recent) > 300:
            self._recent.clear()
        return False

    async def _summary(self, umo: str, hit: dict) -> str:
        extract = re.sub(r"\s+", " ", hit["extract"]).strip()
        limit = self.max_chars
        if not self.use_llm:
            return extract[:limit]
        try:
            provider = self.context.get_using_provider(umo=umo)
        except Exception:  # noqa: BLE001
            provider = None
        if provider is None:
            return extract[:limit]
        persona = await self._persona_prompt(umo)
        system = persona or "你是个懂二次元的群友，说话自然、简短、不端着。"
        prompt = (
            f"有人问你「{hit['title']}」是什么。下面是萌娘百科条目开头的原文：\n\n"
            f"{extract[:1200]}\n\n"
            f"用你平时的口吻回答他：一两句话，不超过 {limit} 字。"
            "保持你自己的性格、语气和说话习惯，就像平时在群里聊天那样，"
            "别切换成百科腔、别写得像念稿、别端起来。"
            "只依据上面的资料，不要补充资料里没有的信息，不要编。"
            "直接说它是什么，不要用「根据资料」「萌娘百科」「这条目」这类开头，不要分点。"
        )
        try:
            resp = await asyncio.wait_for(
                provider.text_chat(
                    prompt=prompt,
                    system_prompt=system,
                    contexts=[],
                ),
                timeout=45,
            )
            text = (getattr(resp, "completion_text", "") or "").strip().strip('"“”「」')
            if text:
                return text[: limit * 2]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[moe_wiki] 总结失败，改用原文: {e}")
        return extract[:limit]

    async def _render(self, event: AstrMessageEvent, hit: dict) -> str:
        body = await self._summary(self._umo(event), hit)
        if not body:
            return ""
        lines = [f"【{hit['title']}】{body}"]
        if self.with_link:
            lines.append(f"{SITE}{quote(hit['title'])}")
        return "\n".join(lines)

    # ---------------- 事件

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        """自动识别问句。查不到就安静走开，把话留给主流程。"""
        if not self.enabled or not self.auto_detect:
            return
        if self.require_at and not self._at_me(event):
            return
        text = str(getattr(event, "message_str", "") or "")
        keyword = extract_entity(text)
        if not keyword:
            return
        if not self._allowed(event):
            return
        key = f"{self._umo(event)}|{keyword}"
        if self._cooling(key):
            return
        hit = await self._lookup(keyword)
        if not hit:
            logger.info(f"[moe_wiki] 没查到「{keyword}」，不插嘴")
            return
        reply = await self._render(event, hit)
        if not reply:
            return
        yield event.plain_result(reply)
        event.stop_event()  # 已经答完了，别让主流程再说一遍

    @filter.command("萌百")
    async def cmd_lookup(self, event: AstrMessageEvent, keyword: str = ""):
        """显式查询：/萌百 初音未来"""
        if not self.enabled:
            yield event.plain_result("萌百问答没开。")
            return
        if not self._allowed(event):
            yield event.plain_result("这个号/这个群没开通萌百查询。")
            return
        keyword = (keyword or "").strip()
        if not keyword:
            yield event.plain_result("用法：/萌百 <词条名>，比如 /萌百 初音未来")
            return
        hit = await self._lookup(keyword)
        if not hit:
            yield event.plain_result(f"萌娘百科上没有「{keyword}」，换个说法试试。")
            return
        reply = await self._render(event, hit)
        if reply:
            yield event.plain_result(reply)
        event.stop_event()

    @filter.command("萌百状态")
    async def cmd_status(self, event: AstrMessageEvent):
        """看看白名单和开关现在是什么样。"""
        qq = ", ".join(sorted(self.allowed_qq)) or "不限（谁都能用）"
        groups = ", ".join(sorted(self.allowed_groups)) or "不限"
        yield event.plain_result(
            f"自动识别：{'开' if self.auto_detect else '关'}\n"
            f"模型总结：{'开' if self.use_llm else '关'}\n"
            f"限定QQ：{qq}\n"
            f"限定群：{groups}\n"
            f"摘要上限：{self.max_chars} 字"
        )

    async def terminate(self):
        await self._wiki.close()
        logger.info("[moe_wiki] 已停止")
