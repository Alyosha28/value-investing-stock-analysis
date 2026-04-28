"""
图表引擎 — 趋势分析专用图表输出

支持两种渲染器：
  1. MatplotlibChartRenderer — 静态5面板K线图（零额外依赖）
  2. PlotlyChartRenderer — 交互式HTML图表（需pip install plotly）
  3. ChartEngine — 外观类，自动选择渲染器
"""

import os
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

import numpy as np
import pandas as pd

from config import TrendConfig
from logger_config import logger

OUTPUT_DIR = TrendConfig.OUTPUT_DIR


# ═══════════════════════════════════════════════════════════════
# 辅助：配置字体
# ═══════════════════════════════════════════════════════════════

def _setup_chinese_font():
    """尝试加载中文字体。"""
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm

    font_candidates = [
        'C:/Windows/Fonts/msyh.ttc',      # 微软雅黑
        'C:/Windows/Fonts/simsun.ttc',     # 宋体
        'C:/Windows/Fonts/mingliu.ttc',    # 细明体
        'C:/Windows/Fonts/SimHei.ttf',    # 黑体
    ]
    for fp in font_candidates:
        if os.path.exists(fp):
            try:
                prop = fm.FontProperties(fname=fp)
                plt.rcParams['font.family'] = prop.get_name()
                plt.rcParams['axes.unicode_minus'] = False
                return prop
            except Exception:
                continue
    # fallback — 禁用中文
    plt.rcParams['axes.unicode_minus'] = False
    return None


def _to_hex_color(rgb_tuple):
    """将matplotlib RGB元组转为十六进制颜色。"""
    r, g, b = rgb_tuple
    return f'#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}'


# ═══════════════════════════════════════════════════════════════
# Matplotlib 渲染器（静态5面板K线图）
# ═══════════════════════════════════════════════════════════════

class MatplotlibChartRenderer:
    """使用matplotlib绘制静态5面板K线图。零额外依赖。"""

    def __init__(self, output_dir=OUTPUT_DIR):
        self.output_dir = output_dir
        self.font_prop = None
        self._setup()

    def _setup(self):
        import matplotlib
        matplotlib.use('Agg')
        self.font_prop = _setup_chinese_font()

    def render(self, data: pd.DataFrame, indicators: Dict, signals: Dict,
               turning_points: List[Dict], sr_levels: Dict,
               stock_name: str, stock_code: str, period: str) -> str:
        """
        渲染图表。

        返回: 图表文件路径
        """
        import matplotlib.pyplot as plt

        close = data['Close']
        has_ohlc = all(c in data.columns for c in ['Open', 'High', 'Low'])

        fig, axes = plt.subplots(5, 1, figsize=(16, 14),
                                 gridspec_kw={'height_ratios': [3, 1, 1, 1.5, 1]})
        ax1, ax2, ax3, ax4, ax5 = axes

        # ── 面板1: K线图 + MA + 信号 + 拐点 + S/R ──
        self._draw_candlestick(ax1, data, close, has_ohlc)
        self._draw_moving_averages(ax1, data, indicators)
        self._draw_signal_markers(ax1, data, signals)
        self._draw_turning_points(ax1, data, turning_points)
        self._draw_sr_levels(ax1, sr_levels)

        title = TrendConfig.CHART_LABELS.get('price', '价格')
        period_label = {'daily': '日K', 'weekly': '周K', 'monthly': '月K'}.get(period, period)
        ax1.set_title(f'{stock_name}({stock_code}) {period_label}趋势分析', fontsize=14, fontproperties=self.font_prop)
        ax1.set_ylabel(title, fontproperties=self.font_prop)
        ax1.legend(prop=self.font_prop, loc='best')
        ax1.grid(True, alpha=0.3)

        # ── 面板2: 成交量 ──
        self._draw_volume(ax2, data, has_ohlc)
        vol_label = TrendConfig.CHART_LABELS.get('volume', '成交量')
        ax2.set_ylabel(vol_label, fontproperties=self.font_prop)
        ax2.grid(True, alpha=0.3)

        # ── 面板3: RSI ──
        self._draw_rsi(ax3, indicators)
        rsi_label = TrendConfig.CHART_LABELS.get('rsi', 'RSI')
        ax3.set_ylabel(rsi_label, fontproperties=self.font_prop)
        ax3.grid(True, alpha=0.3)

        # ── 面板4: MACD ──
        self._draw_macd(ax4, indicators)
        macd_label = TrendConfig.CHART_LABELS.get('macd', 'MACD')
        ax4.set_ylabel(macd_label, fontproperties=self.font_prop)
        ax4.grid(True, alpha=0.3)

        # ── 面板5: KDJ ──
        self._draw_kdj(ax5, indicators)
        kdj_label = TrendConfig.CHART_LABELS.get('kdj', 'KDJ')
        ax5.set_ylabel(kdj_label, fontproperties=self.font_prop)
        ax5.grid(True, alpha=0.3)

        plt.tight_layout()

        period_suffix = {'daily': '日K', 'weekly': '周K', 'monthly': '月K'}.get(period, period)
        os.makedirs(self.output_dir, exist_ok=True)
        chart_path = os.path.join(self.output_dir, f'{stock_code}_{stock_name}_{period_suffix}趋势分析.png')
        plt.savefig(chart_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        logger.info(f"[MatplotlibChart] 已保存: {chart_path}")
        return chart_path

    def _draw_candlestick(self, ax, data, close, has_ohlc):
        """绘制K线实体或折线（当OHLC缺失时）。"""
        import matplotlib.patches as mpatches
        if has_ohlc:
            open_v = data['Open'].values
            high = data['High'].values
            low = data['Low'].values
            close_v = data['Close'].values
            dates_num = np.arange(len(data))

            for i in range(len(data)):
                color = 'green' if close_v[i] >= open_v[i] else 'red'
                # 影线
                ax.plot([dates_num[i], dates_num[i]], [low[i], high[i]],
                        color='black', linewidth=0.8)
                # 实体
                body_height = abs(close_v[i] - open_v[i])
                body_bottom = min(open_v[i], close_v[i])
                rect = mpatches.Rectangle(
                    (dates_num[i] - 0.3, body_bottom), 0.6, body_height,
                    facecolor=color, edgecolor=color, alpha=0.8,
                )
                ax.add_patch(rect)

            ax.set_xlim(-0.5, len(data) - 0.5)
            tick_step = max(1, len(data) // 8)
            ax.set_xticks(range(0, len(data), tick_step))
            ax.set_xticklabels([str(data.index[i].date()) if hasattr(data.index[i], 'date')
                                else str(data.index[i]) for i in range(0, len(data), tick_step)],
                               rotation=30, ha='right', fontsize=8)
        else:
            ax.plot(close.index, close.values, color='blue', linewidth=1.5, label='收盘价')

    def _draw_moving_averages(self, ax, data, indicators):
        """叠加MA均线。"""
        ma_data = indicators.get('ma', {})
        colors = ['orange', 'purple', 'green', 'red']
        for i, (key, ma_series) in enumerate(ma_data.items()):
            if ma_series is not None and not ma_series.empty:
                label = key.upper() if key.islower() else key
                color = colors[i % len(colors)]
                ax.plot(range(len(ma_series)), ma_series.values,
                        color=color, linewidth=1.0, alpha=0.7, label=label)

    def _draw_signal_markers(self, ax, data, signals):
        """在价格面板标记买卖信号。"""
        raw_signals = signals.get('signals', {}) if isinstance(signals, dict) else {}
        composite = raw_signals.get('composite_signal', 0)
        if composite == 0:
            return

        buy_label = TrendConfig.CHART_LABELS.get('buy', '买入')
        sell_label = TrendConfig.CHART_LABELS.get('sell', '卖出')
        close = data['Close'].values
        dates_num = np.arange(len(data))

        # 在最后3根K线标记
        for i in range(max(0, len(data) - 3), len(data)):
            signal_val = raw_signals.get(f'signal_{i}', composite)
            if signal_val >= 1:
                ax.scatter(dates_num[i], close[i] * 0.98, marker='^',
                          color='red', s=120, zorder=5, label=buy_label if i == len(data) - 1 else '')
            elif signal_val <= -1:
                ax.scatter(dates_num[i], close[i] * 1.02, marker='v',
                          color='green', s=120, zorder=5, label=sell_label if i == len(data) - 1 else '')

    def _draw_turning_points(self, ax, data, turning_points):
        """标记关键拐点。"""
        if not turning_points:
            return
        tp_label = TrendConfig.CHART_LABELS.get('turning_point', '拐点')
        close = data['Close'].values
        dates_num = np.arange(len(data))
        dates_list = list(data.index)

        for tp in turning_points:
            tp_date = tp.get('date', '')
            tp_type = tp.get('type', '')
            price = tp.get('price', 0)

            # 找对应索引
            idx = None
            for i, d in enumerate(dates_list):
                d_str = str(d.date()) if hasattr(d, 'date') else str(d)
                if d_str == tp_date:
                    idx = i
                    break

            if idx is None:
                continue

            if 'top' in tp_type:
                ax.scatter(idx, price * 1.02, marker='v', color='red', s=80, zorder=5)
                ax.annotate(tp_label, (idx, price * 1.05), fontsize=7,
                           ha='center', color='red', fontproperties=self.font_prop)
            elif 'bottom' in tp_type:
                ax.scatter(idx, price * 0.98, marker='^', color='green', s=80, zorder=5)
                ax.annotate(tp_label, (idx, price * 0.92), fontsize=7,
                           ha='center', color='green', fontproperties=self.font_prop)

    def _draw_sr_levels(self, ax, sr_levels):
        """绘制支撑/阻力线。"""
        if not sr_levels:
            return
        support_label = TrendConfig.CHART_LABELS.get('support', '支撑')
        resistance_label = TrendConfig.CHART_LABELS.get('resistance', '阻力')

        support = sr_levels.get('support_level')
        resistance = sr_levels.get('resistance_level')

        if support:
            ax.axhline(y=support, color='green', linestyle='--', alpha=0.5, linewidth=1, label=support_label)
        if resistance:
            ax.axhline(y=resistance, color='red', linestyle='--', alpha=0.5, linewidth=1, label=resistance_label)

    def _draw_volume(self, ax, data, has_ohlc):
        """绘制成交量柱状图。"""
        if 'Volume' not in data.columns:
            ax.text(0.5, 0.5, '无成交量数据', transform=ax.transAxes, ha='center', fontproperties=self.font_prop)
            return

        volume = data['Volume'].values
        dates_num = np.arange(len(data))

        if has_ohlc:
            colors = ['green' if data['Close'].iloc[i] >= data['Open'].iloc[i] else 'red'
                      for i in range(len(data))]
        else:
            colors = ['blue'] * len(data)

        ax.bar(dates_num, volume, color=colors, alpha=0.6, width=0.8)
        ax.set_xlim(-0.5, len(data) - 0.5)
        tick_step = max(1, len(data) // 8)
        ax.set_xticks(range(0, len(data), tick_step))

    def _draw_rsi(self, ax, indicators):
        """绘制RSI。"""
        rsi = indicators.get('rsi')
        if rsi is None or rsi.empty:
            ax.text(0.5, 0.5, '无RSI数据', transform=ax.transAxes, ha='center', fontproperties=self.font_prop)
            return

        rsi_vals = rsi.values[-len(rsi):]
        ax.plot(range(len(rsi_vals)), rsi_vals, color='purple', linewidth=1.2)
        ax.axhline(70, linestyle='--', color='red', alpha=0.5)
        ax.axhline(30, linestyle='--', color='green', alpha=0.5)
        ax.fill_between(range(len(rsi_vals)), 70, 30, alpha=0.05, color='gray')
        ax.set_ylim(0, 100)

    def _draw_macd(self, ax, indicators):
        """绘制MACD。"""
        macd = indicators.get('macd')
        if macd is None:
            ax.text(0.5, 0.5, '无MACD数据', transform=ax.transAxes, ha='center', fontproperties=self.font_prop)
            return

        macd_line = macd.get('macd_line')
        signal_line = macd.get('signal_line')
        histogram = macd.get('histogram')

        if macd_line is not None and not macd_line.empty:
            vals = macd_line.values
            ax.plot(range(len(vals)), vals, label='MACD', color='blue', linewidth=1.2)
        if signal_line is not None and not signal_line.empty:
            vals = signal_line.values
            ax.plot(range(len(vals)), vals, label='Signal', color='red', linewidth=1.2)
        if histogram is not None and not histogram.empty:
            vals = histogram.values
            colors = ['green' if v >= 0 else 'red' for v in vals]
            ax.bar(range(len(vals)), vals, color=colors, alpha=0.4, width=0.8)

    def _draw_kdj(self, ax, indicators):
        """绘制KDJ。"""
        kdj = indicators.get('kdj')
        if kdj is None:
            ax.text(0.5, 0.5, '无KDJ数据', transform=ax.transAxes, ha='center', fontproperties=self.font_prop)
            return

        k = kdj.get('k')
        d = kdj.get('d')
        j = kdj.get('j')

        if k is not None and not k.empty:
            ax.plot(range(len(k)), k.values, label='K', color='blue', linewidth=1)
        if d is not None and not d.empty:
            ax.plot(range(len(d)), d.values, label='D', color='red', linewidth=1)
        if j is not None and not j.empty:
            ax.plot(range(len(j)), j.values, label='J', color='green', linewidth=1, linestyle='--')

        ax.axhline(100, linestyle='--', color='gray', alpha=0.3)
        ax.axhline(0, linestyle='--', color='gray', alpha=0.3)
        ax.axhline(80, linestyle=':', color='red', alpha=0.3)
        ax.axhline(20, linestyle=':', color='green', alpha=0.3)


# ═══════════════════════════════════════════════════════════════
# Plotly 渲染器（交互式HTML图表）
# ═══════════════════════════════════════════════════════════════

class PlotlyChartRenderer:
    """使用plotly生成交互式HTML趋势图表。"""

    def __init__(self, output_dir=OUTPUT_DIR):
        self.output_dir = output_dir
        self._plotly = None
        self._available = self._check_available()

    def _check_available(self) -> bool:
        try:
            import plotly.graph_objects as go
            import plotly.subplots as sp
            self._plotly = (go, sp)
            return True
        except ImportError:
            logger.warning("[PlotlyChart] plotly未安装，请 pip install plotly")
            return False

    @property
    def available(self) -> bool:
        return self._available

    def render(self, data: pd.DataFrame, indicators: Dict, signals: Dict,
               turning_points: List[Dict], sr_levels: Dict,
               stock_name: str, stock_code: str, period: str) -> Optional[str]:
        """渲染交互式HTML图表。返回文件路径或None。"""
        if not self._available:
            return None

        go, sp = self._plotly
        max_bars = TrendConfig.CHART_MAX_BARS_PLOTLY

        # 降采样
        if len(data) > max_bars:
            data = self._lttb_downsample(data, max_bars)
            logger.info(f"[PlotlyChart] 降采样: {len(data)} 根K线")

        # 构建面板
        fig = sp.make_subplots(
            rows=5, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            row_heights=[0.35, 0.12, 0.12, 0.18, 0.12],
            subplot_titles=(f'{stock_name}({stock_code})', '成交量', 'RSI', 'MACD', 'KDJ'),
        )

        # 面板1: K线图
        self._add_candlestick_trace(fig, data, 1)
        self._add_ma_traces(fig, data, indicators, 1)
        self._add_sr_traces(fig, sr_levels, 1)
        self._add_turning_point_traces(fig, data, turning_points, 1)

        # 面板2: 成交量
        self._add_volume_trace(fig, data, 2)

        # 面板3: RSI
        self._add_rsi_trace(fig, indicators, 3)

        # 面板4: MACD
        self._add_macd_trace(fig, indicators, 4)

        # 面板5: KDJ
        self._add_kdj_trace(fig, indicators, 5)

        # Range Slider & Selector
        fig.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label='1月', step='month', stepmode='backward'),
                        dict(count=3, label='3月', step='month', stepmode='backward'),
                        dict(count=6, label='6月', step='month', stepmode='backward'),
                        dict(count=1, label='1年', step='year', stepmode='backward'),
                        dict(count=3, label='3年', step='year', stepmode='backward'),
                        dict(step='all'),
                    ]),
                ),
                rangeslider=dict(visible=False),
            ),
            height=900,
            showlegend=True,
            hovermode='x unified',
            template='plotly_white',
        )

        # 输出
        os.makedirs(self.output_dir, exist_ok=True)
        period_label = {'daily': '日K', 'weekly': '周K', 'monthly': '月K'}.get(period, period)
        filepath = os.path.join(self.output_dir, f'{stock_code}_{stock_name}_{period_label}趋势分析.html')
        fig.write_html(filepath, include_plotlyjs='cdn', full_html=True)
        logger.info(f"[PlotlyChart] 已保存: {filepath}")
        return filepath

    def _lttb_downsample(self, data: pd.DataFrame, max_buckets: int) -> pd.DataFrame:
        """LTTB降采样算法。"""
        if len(data) <= max_buckets:
            return data

        n = len(data)
        bucket_size = (n - 2) / (max_buckets - 2)
        sampled = [0]
        a = 0
        for i in range(1, max_buckets - 1):
            bucket_start = int((i - 1) * bucket_size) + 1
            bucket_end = int(i * bucket_size) + 1
            avg_x = (bucket_start + bucket_end) / 2
            avg_y = np.mean(data['Close'].values[bucket_start:bucket_end])
            # 选距(a, avg)连线最远的点
            b = bucket_start
            max_area = -1
            for j in range(bucket_start, bucket_end):
                area = abs((data.index[j] - data.index[a]).total_seconds() * avg_y -
                           (data['Close'].values[j] - avg_y) * (data.index[a]).total_seconds())
                if area > max_area:
                    max_area = area
                    b = j
            sampled.append(b)
            a = b
        sampled.append(n - 1)
        return data.iloc[sampled]

    def _add_candlestick_trace(self, fig, data, row):
        go = self._plotly[0]
        fig.add_trace(
            go.Candlestick(
                x=data.index,
                open=data['Open'],
                high=data['High'],
                low=data['Low'],
                close=data['Close'],
                name='K线',
                increasing_line_color='green',
                decreasing_line_color='red',
            ),
            row=row, col=1,
        )

    def _add_ma_traces(self, fig, data, indicators, row):
        go = self._plotly[0]
        ma_data = indicators.get('ma', {})
        colors = ['orange', 'purple', 'green', 'red']
        for i, (key, ma_series) in enumerate(ma_data.items()):
            if ma_series is not None and not ma_series.empty:
                label = key.upper() if key.islower() else key
                color = colors[i % len(colors)]
                fig.add_trace(
                    go.Scatter(x=ma_series.index, y=ma_series.values,
                              mode='lines', name=label, line=dict(color=color, width=1)),
                    row=row, col=1,
                )

    def _add_sr_traces(self, fig, sr_levels, row):
        go = self._plotly[0]
        support = sr_levels.get('support_level')
        resistance = sr_levels.get('resistance_level')
        if support:
            fig.add_hline(y=support, line_dash='dash', line_color='green',
                         opacity=0.5, annotation_text='支撑', row=row, col=1)
        if resistance:
            fig.add_hline(y=resistance, line_dash='dash', line_color='red',
                         opacity=0.5, annotation_text='阻力', row=row, col=1)

    def _add_turning_point_traces(self, fig, data, turning_points, row):
        go = self._plotly[0]
        if not turning_points:
            return
        for tp in turning_points:
            try:
                tp_date = pd.Timestamp(tp['date'])
                price = tp['price']
                tp_type = tp.get('type', '')
                marker = dict(symbol='triangle-down' if 'top' in tp_type else 'triangle-up',
                            size=10, color='red' if 'top' in tp_type else 'green')
                fig.add_trace(
                    go.Scatter(x=[tp_date], y=[price],
                              mode='markers+text',
                              marker=marker,
                              name=tp_type,
                              text=[tp_type],
                              textposition='top center',
                              showlegend=False),
                    row=row, col=1,
                )
            except Exception:
                continue

    def _add_volume_trace(self, fig, data, row):
        go = self._plotly[0]
        if 'Volume' not in data.columns:
            return
        colors = ['green' if data['Close'].iloc[i] >= data['Open'].iloc[i] else 'red'
                  for i in range(len(data))]
        fig.add_trace(
            go.Bar(x=data.index, y=data['Volume'], name='成交量',
                  marker_color=colors, opacity=0.6),
            row=row, col=1,
        )

    def _add_rsi_trace(self, fig, indicators, row):
        go = self._plotly[0]
        rsi = indicators.get('rsi')
        if rsi is None or rsi.empty:
            return
        fig.add_trace(
            go.Scatter(x=rsi.index, y=rsi.values, mode='lines',
                      name='RSI', line=dict(color='purple', width=1.5)),
            row=row, col=1,
        )
        fig.add_hline(y=70, line_dash='dash', line_color='red', opacity=0.5, row=row, col=1)
        fig.add_hline(y=30, line_dash='dash', line_color='green', opacity=0.5, row=row, col=1)
        fig.update_yaxes(range=[0, 100], row=row, col=1)

    def _add_macd_trace(self, fig, indicators, row):
        go = self._plotly[0]
        macd = indicators.get('macd')
        if macd is None:
            return
        macd_line = macd.get('macd_line')
        signal_line = macd.get('signal_line')
        histogram = macd.get('histogram')

        if macd_line is not None and not macd_line.empty:
            fig.add_trace(
                go.Scatter(x=macd_line.index, y=macd_line.values,
                          mode='lines', name='MACD', line=dict(color='blue')),
                row=row, col=1,
            )
        if signal_line is not None and not signal_line.empty:
            fig.add_trace(
                go.Scatter(x=signal_line.index, y=signal_line.values,
                          mode='lines', name='Signal', line=dict(color='red')),
                row=row, col=1,
            )
        if histogram is not None and not histogram.empty:
            colors = ['green' if v >= 0 else 'red' for v in histogram.values]
            fig.add_trace(
                go.Bar(x=histogram.index, y=histogram.values, name='Histogram',
                      marker_color=colors, opacity=0.4),
                row=row, col=1,
            )

    def _add_kdj_trace(self, fig, indicators, row):
        go = self._plotly[0]
        kdj = indicators.get('kdj')
        if kdj is None:
            return
        k = kdj.get('k')
        d = kdj.get('d')
        j = kdj.get('j')
        if k is not None and not k.empty:
            fig.add_trace(go.Scatter(x=k.index, y=k.values, mode='lines', name='K',
                                    line=dict(color='blue')), row=row, col=1)
        if d is not None and not d.empty:
            fig.add_trace(go.Scatter(x=d.index, y=d.values, mode='lines', name='D',
                                    line=dict(color='red')), row=row, col=1)
        if j is not None and not j.empty:
            fig.add_trace(go.Scatter(x=j.index, y=j.values, mode='lines', name='J',
                                    line=dict(color='green', dash='dash')), row=row, col=1)
        fig.add_hline(y=80, line_dash='dot', line_color='red', opacity=0.3, row=row, col=1)
        fig.add_hline(y=20, line_dash='dot', line_color='green', opacity=0.3, row=row, col=1)


# ═══════════════════════════════════════════════════════════════
# 外观类
# ═══════════════════════════════════════════════════════════════

class ChartEngine:
    """图表引擎外观类，自动选择渲染器。"""

    def __init__(self, output_dir=OUTPUT_DIR):
        self.output_dir = output_dir
        self._matplotlib = MatplotlibChartRenderer(output_dir)
        self._plotly = PlotlyChartRenderer(output_dir)

    def generate(self, stock_data: pd.DataFrame, indicators: Dict,
                 signals: Dict, turning_points: List[Dict],
                 sr_levels: Dict, stock_name: str, stock_code: str,
                 period: str = 'daily', interactive: bool = True) -> str:
        """
        生成趋势图表。

        参数:
            interactive: True→plotly(如可用)/False→matplotlib

        返回:
            图表文件路径
        """
        if interactive and self._plotly.available:
            path = self._plotly.render(stock_data, indicators, signals,
                                       turning_points, sr_levels,
                                       stock_name, stock_code, period)
            if path:
                return path
            logger.info("[ChartEngine] plotly不可用，回退到matplotlib")

        return self._matplotlib.render(stock_data, indicators, signals,
                                       turning_points, sr_levels,
                                       stock_name, stock_code, period)
