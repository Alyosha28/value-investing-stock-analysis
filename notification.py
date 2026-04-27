import os
import smtplib
import requests
import json
import hmac
import hashlib
import base64
import re
import time
from email.mime.text import MIMEText
from email.header import Header
from typing import Dict, Optional, Any, List
from logger_config import logger


class NotificationConfig:
    """从环境变量读取推送配置，提供完整默认值"""

    # 飞书
    FEISHU_WEBHOOK = os.getenv('FEISHU_WEBHOOK', '')
    FEISHU_SECRET = os.getenv('FEISHU_SECRET', '')
    FEISHU_MAX_BYTES = int(os.getenv('FEISHU_MAX_BYTES', '20000'))

    # SMTP 邮箱
    SMTP_HOST = os.getenv('SMTP_HOST', '')
    SMTP_PORT = int(os.getenv('SMTP_PORT', '465'))
    SMTP_USER = os.getenv('SMTP_USER', '')
    SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')
    SMTP_TO = os.getenv('SMTP_TO', '')
    SMTP_SSL = os.getenv('SMTP_SSL', 'true').lower() == 'true'
    EMAIL_MAX_BYTES = int(os.getenv('EMAIL_MAX_BYTES', '50000'))

    # PushPlus
    PUSHPLUS_TOKEN = os.getenv('PUSHPLUS_TOKEN', '')
    PUSHPLUS_TOPIC = os.getenv('PUSHPLUS_TOPIC', '')

    # Server酱3
    SERVERCHAN3_SENDKEY = os.getenv('SERVERCHAN3_SENDKEY', '')

    # 自定义 Webhook
    CUSTOM_WEBHOOK_URL = os.getenv('CUSTOM_WEBHOOK_URL', '')
    CUSTOM_WEBHOOK_BEARER_TOKEN = os.getenv('CUSTOM_WEBHOOK_BEARER_TOKEN', '')
    WEBHOOK_VERIFY_SSL = os.getenv('WEBHOOK_VERIFY_SSL', 'true').lower() == 'true'

    # 行为配置
    SINGLE_STOCK_NOTIFY = os.getenv('SINGLE_STOCK_NOTIFY', 'false').lower() == 'true'
    NOTIFY_TIMEOUT = int(os.getenv('NOTIFY_TIMEOUT', '15'))


class Notifier:
    """
    多渠道通知推送器

    支持飞书机器人、SMTP 邮箱、PushPlus、Server酱3、自定义 Webhook。
    按需调用，无定时任务，不产生冗余消息。
    """

    def __init__(self):
        self.cfg = NotificationConfig()

        # 汇总可用渠道，用于日志提示
        self._channels = []
        if self.cfg.FEISHU_WEBHOOK:
            self._channels.append('飞书')
        if self.cfg.SMTP_HOST and self.cfg.SMTP_USER:
            self._channels.append('邮箱')
        if self.cfg.PUSHPLUS_TOKEN:
            self._channels.append('PushPlus')
        if self.cfg.SERVERCHAN3_SENDKEY:
            self._channels.append('Server酱')
        if self.cfg.CUSTOM_WEBHOOK_URL:
            self._channels.append('自定义Webhook')

        if self._channels:
            logger.info(f"通知推送器初始化完成，可用渠道: {', '.join(self._channels)}")
        else:
            logger.warning("通知推送器初始化完成，未配置任何推送渠道")

    # ------------------------------------------------------------------
    # 公共推送接口
    # ------------------------------------------------------------------

    def send_stock_report(self, result: Dict[str, Any]) -> Dict[str, bool]:
        """
        推送单只股票的分析报告摘要

        Returns:
            {'feishu': bool, 'email': bool, 'pushplus': bool, 'serverchan': bool, 'webhook': bool}
        """
        if not result:
            logger.warning("无分析结果，跳过推送")
            return self._empty_result()

        markdown = self._build_stock_markdown(result)
        html = self._markdown_to_html(markdown)
        subject = f"价值分析简报：{result.get('stock_code', '未知')}"
        return self._push_all(markdown, html, subject)

    def send_screener_summary(self, result: Dict[str, Any]) -> Dict[str, bool]:
        """推送全市场筛选结果摘要"""
        if not result or not result.get('success'):
            logger.warning("筛选失败或无结果，跳过推送")
            return self._empty_result()

        markdown = self._build_screener_markdown(result)
        html = self._markdown_to_html(markdown)
        return self._push_all(markdown, html, subject="全市场价值筛选结果")

    def send_market_regime(self, regime: Dict[str, Any]) -> Dict[str, bool]:
        """推送市场环境分析"""
        markdown = self._build_regime_markdown(regime)
        html = self._markdown_to_html(markdown)
        return self._push_all(markdown, html, subject="市场环境简报")

    # ------------------------------------------------------------------
    # Markdown 构建
    # ------------------------------------------------------------------

    def _build_stock_markdown(self, result: Dict[str, Any]) -> str:
        stock_code = result.get('stock_code', '未知')
        stock_data = result.get('stock_data', {})
        info = stock_data.get('info', {})
        stock_name = info.get('stock_name', '未知')

        graham = result.get('graham_analysis', {})
        buffett = result.get('buffett_analysis', {})
        lynch = result.get('lynch_analysis', {})
        munger = result.get('munger_analysis', {})
        dalio = result.get('dalio_analysis', {})
        technical = result.get('technical_analysis', {})
        ai = result.get('ai_analysis', {})
        regime = result.get('market_regime', {})

        lines = [
            f"# 价值分析简报：{stock_name}（{stock_code}）",
            "",
            f"> **市场环境**：{regime.get('composite_regime', '未知')} | 建议仓位 {regime.get('recommend_position', 'N/A')}%",
            "",
            "## 多大师评分",
            "",
            "| 大师 | 评分 | 建议 |",
            "|------|------|------|",
            f"| 格雷厄姆 | {graham.get('graham_score', 'N/A')}/100 | {graham.get('suggestion', '无')} |",
            f"| 巴菲特 | {buffett.get('buffett_score', 'N/A')}/100 | {buffett.get('suggestion', '无')} |",
            f"| 彼得·林奇 | {lynch.get('lynch_score', 'N/A')}/100 | {lynch.get('suggestion', '无')} |",
            f"| 查理·芒格 | {munger.get('munger_score', 'N/A')}/100 | {munger.get('suggestion', '无')} |",
            f"| 瑞·达里奥 | {dalio.get('dalio_score', 'N/A')}/100 | {dalio.get('suggestion', '无')} |",
            f"| 技术分析 | {technical.get('composite_score', 'N/A')}/100 | {technical.get('signal_strength', '无')} |",
            "",
        ]

        key_metrics = []
        financial = stock_data.get('financial', {})
        pe = financial.get('pe')
        pb = financial.get('pb')
        roe = financial.get('roe')
        if pe is not None:
            key_metrics.append(f"PE: {pe:.1f}")
        if pb is not None:
            key_metrics.append(f"PB: {pb:.1f}")
        if roe is not None:
            key_metrics.append(f"ROE: {roe:.1f}%")
        if key_metrics:
            lines.append(f"**关键指标**：{' | '.join(key_metrics)}")
            lines.append("")

        if lynch.get('peg') is not None:
            lines.append(f"**PEG**: {lynch['peg']}（分类: {lynch.get('category', '未知')}）")
            lines.append("")

        if munger.get('quality_analysis', {}).get('rating'):
            lines.append(f"**企业质量**: {munger['quality_analysis']['rating']}")
            lines.append("")

        quadrant = dalio.get('all_weather_quadrant', {})
        if quadrant.get('quadrant'):
            lines.append(f"**全天候象限**: {quadrant['quadrant']}")
            lines.append("")

        if ai and ai.get('recommendation'):
            lines.append("## AI 综合建议")
            lines.append(f"**{ai.get('recommendation', '无')}**（置信度: {ai.get('confidence_level', '中')}）")
            lines.append("")
            reasons = ai.get('key_reasons', [])
            if reasons:
                lines.append("**关键理由**:")
                for r in reasons[:3]:
                    lines.append(f"- {r}")
                lines.append("")

        lines.append("---")
        lines.append("*免责声明：本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。*")

        return "\n".join(lines)

    def _build_screener_markdown(self, result: Dict[str, Any]) -> str:
        strategy = result.get('strategy', 'comprehensive')
        stocks = result.get('stocks', [])
        summary = result.get('summary', '').replace('\n', '\n> ')

        lines = [
            f"# 全市场筛选结果（{strategy}）",
            "",
            f"> {summary}",
            "",
            f"**返回数量**: Top {len(stocks)} 只",
            "",
            "## 推荐列表",
            "",
            "| 排名 | 名称 | PE | PB | ROE | 评分 | 建议 |",
            "|------|------|----|----|-----|------|------|",
        ]

        for s in stocks[:15]:
            lines.append(
                f"| {s.get('rank')} | {s.get('stock_name')}({s.get('stock_code')}) "
                f"| {s.get('pe', 'N/A')} | {s.get('pb', 'N/A')} | {s.get('roe', 'N/A')}% "
                f"| {s.get('total_score', 'N/A')} | {s.get('suggestion', '')[:20]} |"
            )

        lines.append("")
        lines.append("---")
        lines.append("*免责声明：本报告仅供参考，不构成投资建议。*")

        return "\n".join(lines)

    def _build_regime_markdown(self, regime: Dict[str, Any]) -> str:
        lines = [
            "# 市场环境简报",
            "",
            f"- **综合判断**: {regime.get('composite_regime', '未知')}",
            f"- **趋势强度**: {regime.get('trend_strength', 'N/A')}/100",
            f"- **波动率状态**: {regime.get('volatility_regime', '未知')}",
            f"- **建议仓位**: {regime.get('recommend_position', 'N/A')}%",
        ]
        bc = regime.get('bullish_count', 0)
        tc = regime.get('total_index_count', 0)
        br = (regime.get('breadth_ratio', 0) or 0) * 100
        if tc:
            lines.append(f"- **市场宽度**: {bc}/{tc} 偏多（广度比率 {br:.1f}%）")
        lines.append("")

        index_regimes = regime.get('index_regimes', {})
        if index_regimes:
            lines.append("| 指数 | 最新价 | 涨跌幅 | 趋势阶段 |")
            lines.append("|------|--------|--------|----------|")
            for name, data in index_regimes.items():
                price = data.get('latest_close', 'N/A')
                chg = data.get('change_pct')
                chg_str = f"{chg:+.2f}%" if chg is not None else 'N/A'
                stage = data.get('stage', 'N/A')
                lines.append(f"| {name} | {price} | {chg_str} | {stage} |")
            lines.append("")

        details = regime.get('details', [])
        if not index_regimes and details:
            lines.append("**指数详情**:")
            for d in details:
                lines.append(f"- {d}")
            lines.append("")

        lines.append("---")
        lines.append("*免责声明：仅供参考，不构成投资建议。*")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 统一推送分发
    # ------------------------------------------------------------------

    def _push_all(self, markdown: str, html: str, subject: str) -> Dict[str, bool]:
        results = {}

        if self.cfg.FEISHU_WEBHOOK:
            text = self._truncate(markdown, self.cfg.FEISHU_MAX_BYTES)
            results['feishu'] = self._push_feishu(text)
        else:
            results['feishu'] = False

        if self.cfg.SMTP_HOST and self.cfg.SMTP_USER and self.cfg.SMTP_TO:
            body = self._truncate(html, self.cfg.EMAIL_MAX_BYTES)
            results['email'] = self._push_email(body, subject)
        else:
            results['email'] = False

        if self.cfg.PUSHPLUS_TOKEN:
            text = self._truncate(markdown, 20000)
            results['pushplus'] = self._push_pushplus(text, subject)
        else:
            results['pushplus'] = False

        if self.cfg.SERVERCHAN3_SENDKEY:
            text = self._truncate(markdown, 20000)
            results['serverchan'] = self._push_serverchan(text, subject)
        else:
            results['serverchan'] = False

        if self.cfg.CUSTOM_WEBHOOK_URL:
            text = self._truncate(markdown, 50000)
            results['webhook'] = self._push_custom_webhook(text, subject)
        else:
            results['webhook'] = False

        if not any(results.values()):
            logger.warning("未配置任何推送渠道，消息未发送")

        return results

    def _empty_result(self) -> Dict[str, bool]:
        return {'feishu': False, 'email': False, 'pushplus': False, 'serverchan': False, 'webhook': False}

    @staticmethod
    def _truncate(text: str, max_bytes: int) -> str:
        """按 UTF-8 字节数截断文本，避免超出渠道限制"""
        encoded = text.encode('utf-8')
        if len(encoded) <= max_bytes:
            return text
        # 保守截断，留出省略号空间
        truncated = encoded[:max_bytes - 6]
        # 避免截断在多字节字符中间
        while truncated and (truncated[-1] & 0xC0) == 0x80:
            truncated = truncated[:-1]
        return truncated.decode('utf-8', errors='ignore') + '\n...'

    # ------------------------------------------------------------------
    # 各渠道实现
    # ------------------------------------------------------------------

    def _push_feishu(self, markdown: str) -> bool:
        """飞书机器人推送交互式卡片消息"""
        try:
            timestamp = str(int(time.time()))
            secret = self.cfg.FEISHU_SECRET
            webhook = self.cfg.FEISHU_WEBHOOK

            if secret:
                secret_enc = secret.encode('utf-8')
                string_to_sign = f"{timestamp}\n{secret}".encode('utf-8')
                hmac_code = hmac.new(secret_enc, string_to_sign, digestmod=hashlib.sha256).digest()
                sign = base64.b64encode(hmac_code).decode('utf-8')
            else:
                sign = ''

            payload = {
                "timestamp": timestamp,
                "sign": sign,
                "msg_type": "interactive",
                "card": {
                    "config": {"wide_screen_mode": True},
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": markdown
                            }
                        }
                    ]
                }
            }

            resp = requests.post(webhook, json=payload, timeout=self.cfg.NOTIFY_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if data.get('code') == 0:
                logger.info("飞书推送成功")
                return True
            else:
                logger.error(f"飞书推送失败: {data}")
                return False
        except Exception as e:
            logger.error(f"飞书推送异常: {e}")
            return False

    def _push_email(self, html: str, subject: str) -> bool:
        """SMTP 邮箱推送 HTML 邮件"""
        try:
            msg = MIMEText(html, 'html', 'utf-8')
            msg['From'] = Header(self.cfg.SMTP_USER, 'utf-8')
            msg['To'] = Header(self.cfg.SMTP_TO, 'utf-8')
            msg['Subject'] = Header(subject, 'utf-8')

            if self.cfg.SMTP_SSL:
                server = smtplib.SMTP_SSL(self.cfg.SMTP_HOST, self.cfg.SMTP_PORT)
            else:
                server = smtplib.SMTP(self.cfg.SMTP_HOST, self.cfg.SMTP_PORT)
                server.starttls()

            if self.cfg.SMTP_PASSWORD:
                server.login(self.cfg.SMTP_USER, self.cfg.SMTP_PASSWORD)

            recipients = [r.strip() for r in self.cfg.SMTP_TO.split(',') if r.strip()]
            server.sendmail(self.cfg.SMTP_USER, recipients, msg.as_string())
            server.quit()
            logger.info("邮件推送成功")
            return True
        except Exception as e:
            logger.error(f"邮件推送异常: {e}")
            return False

    def _push_pushplus(self, markdown: str, subject: str) -> bool:
        """PushPlus 微信推送"""
        try:
            payload = {
                "token": self.cfg.PUSHPLUS_TOKEN,
                "title": subject,
                "content": markdown,
                "template": "markdown"
            }
            if self.cfg.PUSHPLUS_TOPIC:
                payload["topic"] = self.cfg.PUSHPLUS_TOPIC

            resp = requests.post(
                "https://www.pushplus.plus/send",
                json=payload,
                timeout=self.cfg.NOTIFY_TIMEOUT,
                verify=self.cfg.WEBHOOK_VERIFY_SSL
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get('code') == 200:
                logger.info("PushPlus 推送成功")
                return True
            else:
                logger.error(f"PushPlus 推送失败: {data}")
                return False
        except Exception as e:
            logger.error(f"PushPlus 推送异常: {e}")
            return False

    def _push_serverchan(self, markdown: str, subject: str) -> bool:
        """Server酱3 微信推送"""
        try:
            payload = {
                "title": subject,
                "content": markdown
            }
            url = f"https://sctapi.ftqq.com/{self.cfg.SERVERCHAN3_SENDKEY}.send"
            resp = requests.post(
                url,
                data=payload,
                timeout=self.cfg.NOTIFY_TIMEOUT,
                verify=self.cfg.WEBHOOK_VERIFY_SSL
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get('code') == 0 or data.get('data', {}).get('error') == 'SUCCESS':
                logger.info("Server酱推送成功")
                return True
            else:
                logger.error(f"Server酱推送失败: {data}")
                return False
        except Exception as e:
            logger.error(f"Server酱推送异常: {e}")
            return False

    def _push_custom_webhook(self, markdown: str, subject: str) -> bool:
        """自定义 Webhook 推送"""
        try:
            payload = {
                "subject": subject,
                "content": markdown,
                "timestamp": int(time.time())
            }
            headers = {'Content-Type': 'application/json'}
            if self.cfg.CUSTOM_WEBHOOK_BEARER_TOKEN:
                headers['Authorization'] = f"Bearer {self.cfg.CUSTOM_WEBHOOK_BEARER_TOKEN}"

            resp = requests.post(
                self.cfg.CUSTOM_WEBHOOK_URL,
                headers=headers,
                json=payload,
                timeout=self.cfg.NOTIFY_TIMEOUT,
                verify=self.cfg.WEBHOOK_VERIFY_SSL
            )
            resp.raise_for_status()
            logger.info("自定义 Webhook 推送成功")
            return True
        except Exception as e:
            logger.error(f"自定义 Webhook 推送异常: {e}")
            return False

    # ------------------------------------------------------------------
    # Markdown -> HTML 转换
    # ------------------------------------------------------------------

    @staticmethod
    def _markdown_to_html(markdown: str) -> str:
        """极简 Markdown -> HTML 转换（支持表格、加粗、标题、列表）"""
        lines = markdown.split('\n')
        html_lines = []
        in_table = False
        table_rows = []

        for line in lines:
            stripped = line.strip()

            if stripped.startswith('# '):
                text = stripped[2:]
                html_lines.append(f"<h1>{text}</h1>")
            elif stripped.startswith('## '):
                text = stripped[3:]
                html_lines.append(f"<h2>{text}</h2>")
            elif stripped.startswith('### '):
                text = stripped[4:]
                html_lines.append(f"<h3>{text}</h3>")
            elif stripped.startswith('> '):
                text = stripped[2:]
                html_lines.append(f"<blockquote>{text}</blockquote>")
            elif stripped.startswith('---'):
                html_lines.append("<hr>")
            elif stripped.startswith('|') and stripped.endswith('|'):
                if not in_table:
                    in_table = True
                    table_rows = []
                table_rows.append(stripped)
            else:
                if in_table:
                    html_lines.append(Notifier._render_table(table_rows))
                    in_table = False
                    table_rows = []
                if stripped.startswith('- '):
                    text = stripped[2:]
                    text = Notifier._inline_md(text)
                    html_lines.append(f"<li>{text}</li>")
                elif stripped:
                    text = Notifier._inline_md(stripped)
                    html_lines.append(f"<p>{text}</p>")

        if in_table and table_rows:
            html_lines.append(Notifier._render_table(table_rows))

        html = "\n".join(html_lines)
        return f"<html><body style='font-family:Arial,sans-serif;line-height:1.6'>{html}</body></html>"

    @staticmethod
    def _render_table(rows: List[str]) -> str:
        if len(rows) < 2:
            return ""
        header = [c.strip() for c in rows[0].strip('|').split('|')]
        body_rows = rows[2:] if len(rows) > 1 and '---' in rows[1] else rows[1:]

        th = "".join(f"<th style='border:1px solid #ccc;padding:6px;background:#f5f5f5'>{c}</th>" for c in header)
        trs = []
        for r in body_rows:
            cells = [c.strip() for c in r.strip('|').split('|')]
            tds = "".join(f"<td style='border:1px solid #ccc;padding:6px'>{c}</td>" for c in cells)
            trs.append(f"<tr>{tds}</tr>")
        tbody = "".join(trs)
        return f"<table style='border-collapse:collapse;margin:8px 0'>{th}{tbody}</table>"

    @staticmethod
    def _inline_md(text: str) -> str:
        text = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', text)
        text = re.sub(r'\*(.*?)\*', r'<em>\1</em>', text)
        text = re.sub(r'`(.*?)`', r'<code>\1</code>', text)
        return text
