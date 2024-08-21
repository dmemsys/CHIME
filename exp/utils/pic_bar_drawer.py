import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
from pathlib import Path
import numpy as np


class BarDrawer(object):

    def __init__(self, pic_dir: str):
        Path(pic_dir).mkdir(exist_ok=True)
        self.pic_dir = pic_dir

    def __load_default_style(self):
        # bar
        self.barHatchDict = {
            'Throughput'        : '/',
            'Invalid Ratio'     : '\\',
            'Delegated Ratio'   : '\\',
            'Combined Ratio'    : '\\',
            'P99 Latency'       : '\\',
            'Lock-fail'         : 'x',

            'CHIME'            : '|||',
            'SMART'            : '',
            'Sherman'          : '\\\\',
            'ROLEX'            : '///',

            'CHIME-indirect'   : '--',
            'SMART-RCU'        : '',
            'Marlin'           : '\\\\',
            'ROLEX-indirect'   : '///',

            'RDMA READ'        : '',
            'Hopscotch'        : '',

            'Baseline (Sherman)'       : '\\\\',
            'Baseline (ROLEX)'         : '///',
            '+Hopscotch Leaf Node'     : '|||',
            '+Vacancy-Aware Lock'      : '--',
            '+Metadata Replication'    : '+++',
            '+Sibling-Based Validation': 'xx',
            '+Speculative Leaf Read' : '',
            '+Speculative Leaf Read (CHIME)' : '',

            'Coarse-Grained Mode': '\\\\',
            'Fine-Grained Mode'  : '',
            '+Greedy Range Query': 'x',
        }
        self.barColorDict = {
            'Throughput'        : '#F8CECC',
            'Invalid Ratio'     : '#DAE8FC',
            'Delegated Ratio'   : '#DAE8FC',
            'Combined Ratio'    : '#DAE8FC',
            'P99 Latency'       : '#DAE8FC',
            'Lock-fail'         : '#D5E8D4',

            'CHIME'            : '#e8cefb',
            'SMART'            : '#D5E8D4',
            'Sherman'          : '#DAE8FC',
            'ROLEX'            : '#ee8fab',

            'CHIME-indirect'   : '#e8cefb',
            'SMART-RCU'        : '#D5E8D4',
            'Marlin'           : '#DAE8FC',
            'ROLEX-indirect'   : '#ee8fab',

            'RDMA READ'        : '#DAE8FC',
            'Hopscotch'        : '#e8cefb',

            'Baseline (Sherman)'        : "#F8CECC",
            'Baseline (ROLEX)'          : "#F8CECC",
            '+Hopscotch Leaf Node'      : "#D5E8D4",
            '+Vacancy-Aware Lock'       : '#D5E8D4',
            '+Metadata Replication'     : '#DAE8FC',
            '+Sibling-Based Validation' : '#DAE8FC',
            '+Speculative Leaf Read'  : '#e8cefb',
            '+Speculative Leaf Read (CHIME)': '#e8cefb',

            'Coarse-Grained Mode': '#DAE8FC',
            'Fine-Grained Mode'  : '#D5E8D4',
            '+Greedy Range Query': '#e8cefb',
        }
        # line
        self.lineStyleDict = {
            'P50 Latency'    : (0, ()),
            'P99 Latency'    : (0, (3, 1)),
            'Cache Hit Ratio': (0, ())
        }
        self.lineColorDict = {
            'P50 Latency'    : '#333333',
            'P99 Latency'    : '#333333',
            'Cache Hit Ratio': '#333333',
        }
        self.lineMarkerDict = {
            'P50 Latency'    : 'o',
            'P99 Latency'    : '^',
            'Cache Hit Ratio': 'o',
        }
        self.zorderDict = {
            'P50 Latency'    : 1200,
            'P99 Latency'    : 1250,
            'Cache Hit Ratio': 1250,
        }
        # size
        self.figsize=(4, 2.5)
        self.font_size = 15
        self.legend_size = 15
        self.tick_size = 14
        self.linewidth = 0.8
        plt.rcParams['hatch.linewidth'] = 0.4
        self.markersize = 6
        # edge
        self.line_clip_on=False
        self.ignore_point = {}
        self.hide_ylabel = False
        self.hide_yRlabel = False
        # tick
        self.yRfloat = False
        # bar size
        self.group_width = 0.8
        self.bar_offset = 0.5
        self.bar_padding = 0
        self.group_width_ratio = 1.0
        # legend
        self.legend_location = ''
        self.legend_anchor = ()
        self.legendL_location = self.legendR_location = ''
        self.legendL_anchor = self.legendR_anchor = ()
        self.legend_ncol = 1
        self.legendL_ncol = self.legendR_ncol = 2
        self.legend_param = {}
        self.legendL_param = self.legendR_param = {}
        self.method_legend = {}
        # tick
        self.x_ticklabel = False
        self.y_lim = self.x_lim = ()
        self.yL_lim = self.yR_lim = ()
        self.x_tick = self.y_tick = []
        self.yL_tick = self.yR_tick = []
        self.yscale = ''
        self.yfloat = False
        self.yR_scale = ''
        # label
        self.x_label = ''
        self.y_label = ''
        self.yL_label = ''
        self.yR_label = ''
        self.y_multiple = 1
        # func
        self.aux_plt_func = None
        self.annotation_func = None
        self.method_ax = {}
        # pile_up
        self.pile_up_value = {}
        self.pile_up_legend = ''
        self.pile_up_color = 'grey'
        self.pile_up_hatch = '--'


    def plot_with_one_ax(self, data: dict, fig_name: str, custom_style: dict = {}):
        self.__load_default_style()
        # load custom style
        for k, v in custom_style.items():
            setattr(self, k, v)

        fig, ax = plt.subplots(1, 1, figsize=self.figsize, dpi=300)
        methods = data['methods']
        bar_groups = data['bar_groups']
        if not self.method_legend:
            self.method_legend = {method: method for method in data['methods']}

        bar_width = self.group_width / len(methods)
        for i, method in enumerate(methods):
            ax.bar(np.arange(len(bar_groups)) + (i - self.bar_offset) * bar_width,
                   list(map(lambda x : float(x) / self.y_multiple, [data['Y_data'][method][g] for g in bar_groups])),
                   width=bar_width,
                   label=self.method_legend[method],
                   color=self.barColorDict[method],
                   edgecolor='black',
                   linewidth=self.linewidth,
                   fill=True,
                   hatch=self.barHatchDict[method],
                   zorder=100)
            if method in self.pile_up_value:
                ax.bar(np.arange(len(bar_groups)) + (i - self.bar_offset) * bar_width,
                       list(map(lambda x : float(x) / self.y_multiple, [self.pile_up_value[method] for g in bar_groups])),
                       bottom=list(map(lambda x : float(x) / self.y_multiple, [data['Y_data'][method][g] for g in bar_groups])),
                       width=bar_width,
                       label=self.pile_up_legend,
                       color=self.pile_up_color,
                       edgecolor='black',
                       linewidth=self.linewidth,
                       fill=True,
                       hatch=self.pile_up_hatch,
                       zorder=100)
        if self.x_label:
            ax.set_xlabel(self.x_label, fontsize=self.font_size)
        ax.set_xticks([i for i in range(len(bar_groups))])
        ax.set_xticklabels(bar_groups, fontsize=self.tick_size)
        ax.grid(axis='y', color='#dbdbdb', lw=0.3, zorder=0)

        ax.set_ylabel(self.y_label, fontsize=self.font_size)
        if self.y_tick:
            ax.set_yticks(self.y_tick)
            ax.set_yticklabels(self.y_tick, fontsize=self.tick_size)
        if self.y_lim:
            ax.set_ylim(*self.y_lim)
        if self.yscale:
            ax.set_yscale(self.yscale)
        if self.legend_anchor:
            if self.legend_param:
                ax.legend(bbox_to_anchor=self.legend_anchor, frameon=False, **self.legend_param)
            else:
                ax.legend(fontsize=self.legend_size, bbox_to_anchor=self.legend_anchor, frameon=False, ncol=self.legend_ncol)
        else:
            ax.legend(fontsize=self.legend_size, loc=self.legend_location, frameon=False)

        plt.gca().spines['bottom'].set_zorder(1000)
        plt.gca().spines['left'].set_zorder(1000)

        if self.aux_plt_func:
            self.aux_plt_func(ax)
        plt.savefig(str(Path(self.pic_dir) / fig_name), format='pdf', bbox_inches='tight')
        plt.close()


    def plot_with_two_ax(self, data: dict, fig_name: str, custom_style: dict = {}):
        self.__load_default_style()
        # load custom style
        for k, v in custom_style.items():
            setattr(self, k, v)

        fig, ax_L = plt.subplots(1, 1, figsize=self.figsize, dpi=300)
        methods = data['methods']
        bar_groups = data['bar_groups']
        if not self.method_legend:
            self.method_legend = {method: method for method in data['methods']}
        if not self.method_ax:
            self.method_ax = dict(zip(data['methods'], ['left', 'right']))

        ax_R = ax_L.twinx()
        bar_width = self.group_width / len(methods)
        for i, method in enumerate(methods):
            ax = ax_L if self.method_ax[method] == 'left' else ax_R
            ax.bar(np.arange(len(bar_groups)) + (i - self.bar_offset) * bar_width,
                   [data['Y_data'][method][g] for g in bar_groups],
                   width=bar_width,
                   label=self.method_legend[method],
                   color=self.barColorDict[method],
                   edgecolor='black',
                   linewidth=self.linewidth,
                   fill=True,
                   hatch=self.barHatchDict[method],
                   zorder=100)
        if self.x_label:
            ax_L.set_xlabel(self.x_label, fontsize=self.font_size)
        ax_L.set_xticks([i for i in range(len(bar_groups))])
        ax_L.set_xticklabels(bar_groups, fontsize=self.tick_size)
        ax_L.grid(axis='y', color='#dbdbdb', lw=0.3, zorder=0)

        ax_L.set_ylabel(self.yL_label, fontsize=self.font_size)
        ax_L.set_yticks(self.yL_tick)
        ax_L.set_yticklabels(self.yL_tick, fontsize=self.font_size)
        if self.legendL_anchor:
            if self.legendL_param:
                ax_L.legend(bbox_to_anchor=self.legendL_anchor, frameon=False, **self.legendL_param)
            else:
                ax_L.legend(fontsize=self.legend_size, bbox_to_anchor=self.legendL_anchor, frameon=False)
        else:
            ax_L.legend(fontsize=self.legend_size, loc=self.legendL_location, frameon=False)
        ax_R.set_ylabel(self.yR_label, fontsize=self.font_size)
        ax_R.set_yticks(self.yR_tick)
        ax_R.set_yticklabels(self.yR_tick, fontsize=self.tick_size)
        if self.legendR_anchor:
            if self.legendR_param:
                ax_R.legend(bbox_to_anchor=self.legendR_anchor, frameon=False, **self.legendR_param)
            else:
                ax_R.legend(fontsize=self.legend_size, bbox_to_anchor=self.legendR_anchor, frameon=False)
        else:
            ax_R.legend(fontsize=self.legend_size, loc=self.legendR_location, frameon=False)
        if self.yRfloat:
            ax_R.yaxis.set_major_formatter(FormatStrFormatter('%.2f'))

        plt.gca().spines['bottom'].set_zorder(1000)
        plt.gca().spines['left'].set_zorder(1000)

        if self.aux_plt_func:
            self.aux_plt_func(ax)
        plt.savefig(str(Path(self.pic_dir) / fig_name), format='pdf', bbox_inches='tight')
        plt.close()


    def plot_with_line(self, data: dict, fig_name: str, custom_style: dict = {}):
        self.__load_default_style()
        # load custom style
        for k, v in custom_style.items():
            setattr(self, k, v)

        fig, ax_L = plt.subplots(1, 1, figsize=self.figsize, dpi=300)
        methods = data['methods']
        bar_groups = data['bar_groups']
        if not self.method_legend:
            self.method_legend = {method: method for method in data['methods']}
        if not self.method_ax:
            self.method_ax = dict(zip(data['methods'], ['left', 'right']))

        line_x_data = {g : [] for g in bar_groups}

        # plot bar
        bar_metric = data['metrics'][0]
        totalWidth = self.group_width * self.group_width_ratio
        barWidth = totalWidth / len(methods)
        for i, method in enumerate(methods):
            bar_x_data = np.arange(len(bar_groups)) + (i - self.bar_offset) * barWidth
            for x, g in zip(bar_x_data, bar_groups):
                if self.ignore_point and self.ignore_point['bar_group'] == g and self.ignore_point['method'] == method:
                    continue
                line_x_data[g].append(x)
            ax_L.bar(bar_x_data,
                     [data['Y_data'][method][g][bar_metric] for g in bar_groups],
                     width=barWidth*(1-self.bar_padding),
                     label=self.method_legend[method],
                     color=self.barColorDict[method],
                     edgecolor='black',
                     linewidth=self.linewidth,
                     fill=True,
                     hatch=self.barHatchDict[method],
                     zorder=100)
        if self.x_label:
            ax_L.set_xlabel(self.x_label, fontsize=self.font_size)
        ax_L.set_xticks([i for i in range(len(bar_groups))])
        ax_L.set_xticklabels(bar_groups, fontsize=self.font_size)
        ax_L.grid(axis='y', color='#dbdbdb', lw=0.3, zorder=0)

        if not self.hide_ylabel:
            ax_L.set_ylabel(self.y_label, fontsize=self.font_size)
        if self.y_tick:
            ax_L.set_yticks(self.y_tick)
        if self.y_lim:
            ax_L.set_ylim(*self.y_lim)
        if self.legend_anchor:
            if self.legend_param:
                ax_L.legend(bbox_to_anchor=self.legend_anchor, frameon=False, **self.legend_param)
            else:
                ax_L.legend(fontsize=self.legend_size, bbox_to_anchor=self.legend_anchor, frameon=False, ncol=self.legend_ncol)
        else:
            ax_L.legend(fontsize=self.legend_size, loc=self.legend_location, frameon=False, ncol=self.legend_ncol, columnspacing=0.8)
        ax_L.tick_params(labelsize=self.tick_size)

        # plot line
        line_metrics = data['metrics'][1:]
        lines_data = []
        for g in bar_groups:
            for m in line_metrics:
                lines_data.append((g, m, [
                    data['Y_data'][method][g][m]
                    for method in methods
                    if not (self.ignore_point and self.ignore_point['bar_group'] == g and self.ignore_point['method'] == method)
                ]))

        ax_R = ax_L.twinx()
        legend_handles = {}
        if len(methods) > 1:
            for g, metric, line_data in lines_data:
                l, = ax_R.plot(line_x_data[g],
                               line_data,
                               linestyle=self.lineStyleDict[metric],
                               color=self.lineColorDict[metric],
                               marker=self.lineMarkerDict[metric],
                               markerfacecolor='white',
                               mew=self.linewidth,
                               clip_on=self.line_clip_on,
                               linewidth=self.linewidth,
                               markersize=self.markersize + 1.5 if self.lineMarkerDict[metric] == '+' else
                                          self.markersize + 0.5 if self.lineMarkerDict[metric] in ['x', '^'] else self.markersize,
                               zorder=self.zorderDict[metric])
                legend_handles[metric] = l
        else:
            for metric, line_data in lines_data:
                l, = ax_R.plot([a[0] for a in line_x_data.values()],
                               line_data,
                               linestyle=self.lineStyleDict[metric],
                               color=self.lineColorDict[metric],
                               marker=self.lineMarkerDict[metric],
                               markerfacecolor='white',
                               mew=self.linewidth,
                               clip_on=self.line_clip_on,
                               linewidth=self.linewidth,
                               markersize=self.markersize + 1.5 if self.lineMarkerDict[metric] == '+' else
                                          self.markersize + 0.5 if self.lineMarkerDict[metric] in ['x', '^'] else self.markersize,
                               zorder=self.zorderDict[metric])
                legend_handles[metric] = l
        if not self.hide_yRlabel:
            ax_R.set_ylabel(self.yR_label, fontsize=self.font_size)
        if self.yR_tick:
            ax_R.set_yticks(self.yR_tick)
        if self.yR_lim:
            ax_R.set_ylim(*self.yR_lim)
        if self.yR_scale:
            ax_R.set_yscale(self.yR_scale)
        ax_R.tick_params(labelsize=self.tick_size)
        if self.legendR_location:
            if self.legendR_param:
                ax_R.legend(legend_handles.values(), legend_handles.keys(), loc=self.legendR_location, frameon=False, **self.legendR_param)
            else:
                ax_R.legend(legend_handles.values(), legend_handles.keys(), fontsize=self.legend_size, loc=self.legendR_location, frameon=False, ncol=self.legendR_ncol)
        else:
            if self.legendR_param:
                ax_R.legend(legend_handles.values(), legend_handles.keys(), bbox_to_anchor=self.legendR_anchor, frameon=False, **self.legendR_param)
            else:
                ax_R.legend(legend_handles.values(), legend_handles.keys(), fontsize=self.legend_size, bbox_to_anchor=self.legendR_anchor, frameon=False, ncol=self.legendR_ncol)

        plt.gca().spines['bottom'].set_zorder(1000)
        plt.gca().spines['left'].set_zorder(1000)

        plt.savefig(str(Path(self.pic_dir) / fig_name), format='pdf', bbox_inches='tight')
        plt.close()