import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
from pathlib import Path
import numpy as np


class LineDrawer(object):

    def __init__(self, pic_dir: str):
        Path(pic_dir).mkdir(exist_ok=True)
        self.pic_dir = pic_dir


    def __load_default_style(self):
        # line
        self.lineStyleDict = {
            'CHIME'          : (0, ()),
            'SMART'          : (0, ()),
            'Sherman'        : (0, ()),
            'ROLEX'          : (0, ()),
            'SMART-SC'       : (0, (5, 3)),

            'Sherman (8-span)'  : (0, ()),
            'Sherman (16-span)' : (0, ()),
            'Sherman (64-span)' : (0, ()),

            'CHIME-indirect' : (0, ()),
            'SMART-RCU'      : (0, ()),
            'Marlin'         : (0, ()),
            'ROLEX-indirect' : (0, ()),
            'SMART-RCU-SC'   : (0, (5, 3)),

            'Associativity' : (0, ()),
            'RACE'          : (0, ()),
            'Hopscotch'     : (0, ()),
            'FaRM'          : (0, ()),

            'optimistic lock' : (0, (5, 3)),
            'pessimistic lock': (0, (5, 3)),
            'optimistic lock + RDWC' : (0, ()),
            'pessimistic lock + RDWC': (0, ()),

            'Segment'                : (0, ()),
            'Metadata before Segment': (0, (5, 3)),
            'Node with Metadata'     : (0, ()),
            'Segment with Metadata'  : (0, ()),
            'Segment and Metadata (2-I/O)' : (0, (5, 3)),

            'Neighborhood (1-entry)' : (0, ()),
            'Neighborhood (2-entry)' : (0, ()),
            'Neighborhood (4-entry)' : (0, ()),
            'Neighborhood (8-entry)' : (0, ()),

            'CHIME w/ Sibling-Based Validation' : (0, ()),
            'CHIME w/o Sibling-Based Validation': (0, ()),
            'CHIME w/ Speculative Leaf Read' : (0, ()),
            'CHIME w/o Speculative Leaf Read': (0, ()),
            'Sherman +Speculative Leaf Read' : (0, ()),

            'Throughput' : (0, ()),
            'P99 Latency': (0, ()),
            'Cache Efficiency': (0, ()),
            'Load Factor'     : (0, ()),
            'Buffer Hit Ratio': (0, ()),
            'Improvement Ratio': (0, (5, 3)),

            'RDMA_READ' : (0, ()),
            'RDMA_WRITE': (0, ()),
            'RDMA_CAS'  : (0, ()),
        }
        self.lineColorDict = {
            'CHIME'          : '#933fd3',
            'SMART'          : '#75a35b',
            'Sherman'        : '#4575B5',
            'ROLEX'          : '#d5116d',
            'SMART-SC'       : '#75a35b',

            'Sherman (8-span)' : '#4575B5',
            'Sherman (16-span)': '#4575B5',
            'Sherman (64-span)': '#4575B5',

            'CHIME-indirect' : '#933fd3',
            'SMART-RCU'      : '#75a35b',
            'Marlin'         : '#4575B5',
            'ROLEX-indirect' : '#d5116d',
            'SMART-RCU-SC'   : '#75a35b',

            'Associativity' : '#ad261e',
            'RACE'          : '#d5116d',
            'Hopscotch'     : '#933fd3',
            'FaRM'          : '#4575B5',

            'optimistic lock' : '#933fd3',
            'pessimistic lock': '#ad261e',
            'optimistic lock + RDWC' : '#933fd3',
            'pessimistic lock + RDWC': '#ad261e',

            'Segment'                : '#933fd3',
            'Metadata before Segment': '#4575B5',
            'Node with Metadata'     : '#ad261e',
            'Segment with Metadata'  : '#4575B5',
            'Segment and Metadata (2-I/O)' : '#4575B5',

            'Neighborhood (1-entry)' : '#933fd3',
            'Neighborhood (2-entry)' : '#4575B5',
            'Neighborhood (4-entry)' : '#4575B5',
            'Neighborhood (8-entry)' : '#4575B5',

            'CHIME w/ Sibling-Based Validation' : '#933fd3',
            'CHIME w/o Sibling-Based Validation': '#4575B5',
            'CHIME w/ Speculative Leaf Read' : '#933fd3',
            'CHIME w/o Speculative Leaf Read': '#4575B5',
            'Sherman +Speculative Leaf Read' : '#4575B5',

            'Throughput': '#933fd3',
            'P99 Latency': '#ad261e',
            'Cache Efficiency': '#4575B5',
            'Load Factor'     : '#ad261e',
            'Buffer Hit Ratio': '#ad261e',
            'Improvement Ratio': '#ad261e',

            'RDMA_READ' : '#ad261e',
            'RDMA_WRITE': '#ad261e',
            'RDMA_CAS'  : '#75a35b',
        }
        self.lineMarkerDict = {
            'CHIME'          : 'd',
            'SMART'          : 's',
            'Sherman'        : 'v',
            'ROLEX'          : 'o',
            'SMART-SC'       : '',

            'Sherman (8-span)' : 'x',
            'Sherman (16-span)': '+',
            'Sherman (64-span)': 'v',

            'CHIME-indirect' : 'd',
            'SMART-RCU'      : 's',
            'Marlin'         : 'v',
            'ROLEX-indirect' : 'o',
            'SMART-RCU-SC'   : '',

            'Associativity' : 'x',
            'RACE'          : 'o',
            'Hopscotch'     : '^',
            'FaRM'          : '+',

            'optimistic lock' : '^',
            'pessimistic lock': 'x',
            'optimistic lock + RDWC' : '^',
            'pessimistic lock + RDWC': 'x',

            'Segment'                : '^',
            'Metadata before Segment': '+',
            'Node with Metadata'             : 'x',
            'Segment with Metadata'  : 'o',
            'Segment and Metadata (2-I/O)' : '+',

            'Neighborhood (1-entry)' : '^',
            'Neighborhood (2-entry)' : 'x',
            'Neighborhood (4-entry)' : '+',
            'Neighborhood (8-entry)' : 'o',

            'CHIME w/ Sibling-Based Validation' : 'd',
            'CHIME w/o Sibling-Based Validation': 'x',
            'CHIME w/ Speculative Leaf Read' : 'd',
            'CHIME w/o Speculative Leaf Read': 'x',
            'Sherman +Speculative Leaf Read' : 'o',

            'Throughput' : 'd',
            'P99 Latency': '^',
            'Cache Efficiency': '+',
            'Load Factor'     : '^',
            'Buffer Hit Ratio': 'o',
            'Improvement Ratio': '^',

            'RDMA_READ' : 'x',
            'RDMA_WRITE': 'x',
            'RDMA_CAS'  : '^',
        }
        self.zorderDict = {
            'CHIME'          : 1350,
            'SMART'          : 1250,
            'Sherman'        : 1150,
            'ROLEX'          : 1050,
            'SMART-SC'       : 900,

            'Sherman (8-span)' : 1300,
            'Sherman (16-span)': 1300,
            'Sherman (64-span)': 1300,

            'CHIME-indirect' : 1350,
            'SMART-RCU'      : 1250,
            'Marlin'         : 1150,
            'ROLEX-indirect' : 1050,
            'SMART-RCU-SC'   : 900,

            'Associativity' : 1100,
            'RACE'          : 1200,
            'Hopscotch'     : 1350,
            'FaRM'          : 1300,

            'optimistic lock' : 1300,
            'pessimistic lock': 1200,
            'optimistic lock + RDWC' : 1100,
            'pessimistic lock + RDWC': 1000,

            'Segment'                : 1350,
            'Metadata before Segment': 1300,
            'Node with Metadata'             : 1100,
            'Segment with Metadata'  : 1320,
            'Segment and Metadata (2-I/O)' : 1330,

            'Neighborhood (1-entry)' : 1050,
            'Neighborhood (2-entry)' : 1150,
            'Neighborhood (4-entry)' : 1250,
            'Neighborhood (8-entry)' : 1350,

            'CHIME w/ Sibling-Based Validation' : 1200,
            'CHIME w/o Sibling-Based Validation': 1100,
            'CHIME w/ Speculative Leaf Read' : 1200,
            'CHIME w/o Speculative Leaf Read': 1100,
            'Sherman +Speculative Leaf Read' : 1100,

            'Throughput' : 1000,
            'P99 Latency': 1000,
            'Cache Efficiency': 1000,
            'Load Factor'     : 1000,
            'Improvement Ratio': 2000,

            'RDMA_READ' : 1000,
            'RDMA_WRITE': 1000,
            'RDMA_CAS'  : 1100,
        }
        # size
        self.figsize=(4, 2.5)
        self.font_size = 15
        self.legend_size = 15
        self.tick_size = 14
        self.linewidth = 0.8
        self.markersize = 6
        # grid
        self.grid_type = {'axis': 'y', 'lw': 0.3}
        self.y_major_num = self.x_major_num = 0
        self.grid_minor = False
        # edge
        self.hide_half_edge = False
        self.hide_ylabel = False
        self.hide_yRlabel = False
        self.clip_on = True
        # legend
        self.legend_location = ''
        self.legend_anchor = ()
        self.legendL_anchor = self.legendR_anchor = ()
        self.legend_ncol = 1
        self.method_legend = {}
        # tick
        self.x_ticklabel = False
        self.y_lim = self.x_lim = ()
        self.ylim = self.xlim = ()
        self.yL_lim = self.yR_lim = ()
        self.x_tick = self.y_tick = []
        self.yL_tick = self.yR_tick = []
        self.yscale = ''
        self.yfloat = False
        # label
        self.x_label = ''
        self.y_label = ''
        self.yL_label = ''
        self.yR_label = ''
        self.y_multiple = 1
        # func
        self.aux_plt_func = None
        self.annotation_func = None
        self.dotted_methods = []
        self.shadow = {}
        self.method_ax = {}


    def plot_with_one_ax(self, data: dict, fig_name: str, custom_style: dict = {}):
        self.__load_default_style()
        # load custom style
        for k, v in custom_style.items():
            setattr(self, k, v)

        fig, ax = plt.subplots(1, 1, figsize=self.figsize, dpi=300)
        if self.hide_half_edge:
            ax.spines['right'].set_visible(False)
            ax.spines['top'].set_visible(False)
        legend_handles = []
        legend_labels  = []
        if not self.method_legend:
            self.method_legend = {method: method for method in data['methods']}

        X_data = data['X_data']
        Y_data = data['Y_data']
        for method in data['methods']:
            l, = ax.plot(X_data[method] if isinstance(X_data, dict) else X_data,
                         list(map(lambda x : float(x) / self.y_multiple, Y_data[method])),
                         linestyle=self.lineStyleDict[method] if method not in self.dotted_methods else (0, (5, 3)),
                         color=self.lineColorDict[method],
                         marker=self.lineMarkerDict[method] if method not in self.dotted_methods else '',
                         markerfacecolor='none',
                         mew=self.linewidth,
                         clip_on=self.clip_on,
                         linewidth=self.linewidth,
                         markersize=self.markersize + 1.5 if self.lineMarkerDict[method] == '+' else
                                    self.markersize + 0.5 if self.lineMarkerDict[method] == 'x' else self.markersize,
                         zorder=self.zorderDict[method])
            if method in self.method_legend:
                legend_handles.append(l)
                legend_labels.append(self.method_legend[method])
        ax.set_xlabel(self.x_label, fontsize=self.font_size)
        if not self.hide_ylabel:
            ax.set_ylabel(self.y_label, fontsize=self.font_size)
        if self.yscale:
            ax.set_yscale(self.yscale)
        if self.yfloat:
            ax.yaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        if self.ylim:
            ytick       = list(np.linspace(self.ylim[0], self.ylim[1], self.y_major_num))
            ytick_minor = list(np.linspace(self.ylim[0], self.ylim[1], 0 if self.grid_minor is False else self.y_major_num * 2 - 1))
            ax.set_yticks(ytick)
            ax.set_yticks(ytick_minor, minor=True)
        if self.xlim:
            xtick       = list(np.linspace(self.xlim[0], self.xlim[1], self.x_major_num))
            xtick_minor = list(np.linspace(self.xlim[0], self.xlim[1], 0 if self.grid_minor is False else self.x_major_num * 2 - 1))
            ax.set_xticks(xtick)
            ax.set_xticks(xtick_minor, minor=True)
        if self.y_tick:
            ax.set_yticks(self.y_tick)
        if self.x_tick:
            ax.set_xticks(self.x_tick)
            if self.x_ticklabel:
                ax.set_xticklabels(self.x_ticklabel, fontsize=self.tick_size)
        if self.y_lim:  # x, y limitation, use when xlim/ylim is not enough
            ax.set_ylim(*self.y_lim)
        if self.x_lim:
            ax.set_xlim(*self.x_lim)
        ax.tick_params(labelsize=self.tick_size)
        # ax.set_xscale('log')
        if self.annotation_func:
            self.annotation_func(ax)
        ax.grid(color='#dbdbdb', **self.grid_type, which='both' if self.grid_minor else 'major', zorder=0)
        if self.legend_location or self.legend_anchor:
            if self.legend_anchor:
                ax.legend(legend_handles, legend_labels, fontsize=self.legend_size, bbox_to_anchor=self.legend_anchor, frameon=False, ncol=self.legend_ncol)
            else:
                ax.legend(legend_handles, legend_labels, fontsize=self.legend_size, loc=self.legend_location, frameon=False, ncol=self.legend_ncol) # labelspacing=0.1
        if self.shadow:
            ax.fill_between(X_data[self.shadow['upper']] if isinstance(X_data, dict) else X_data,
                            Y_data[self.shadow['upper']], Y_data[self.shadow['lower']],
                            where=(np.array(Y_data[self.shadow['upper']]) - np.array(Y_data[self.shadow['lower']]) > 0) if self.shadow['only_higher'] else np.array([True]*len(Y_data[self.shadow['upper']])),
                            color=self.shadow['color'], alpha=self.shadow['alpha'])
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
        ax_R = ax_L.twinx()
        # legend_list = []
        if not self.method_legend:
            self.method_legend = {method: method for method in data['methods']}
        if not self.method_ax:
            self.method_ax = dict(zip(data['methods'], ['left', 'right']))

        X_data = data['X_data']
        Y_data = data['Y_data']
        for method in data['methods']:
            ax = ax_L if self.method_ax[method] == 'left' else ax_R
            ax.plot(X_data[method] if isinstance(X_data, dict) else X_data,
                    list(map(lambda x : float(x) / self.y_multiple, Y_data[method])) if self.method_ax[method] == 'left' else Y_data[method],
                    label=self.method_legend[method],
                    linestyle=self.lineStyleDict[method] if method not in self.dotted_methods else (0, (5, 3)),
                    color=self.lineColorDict[method],
                    marker=self.lineMarkerDict[method],
                    markerfacecolor='none',
                    clip_on=self.clip_on,
                    linewidth=self.linewidth,
                    markersize=self.markersize + 1.5 if self.lineMarkerDict[method] == '+' else
                               self.markersize + 0.5 if self.lineMarkerDict[method] == 'x' else self.markersize)
            # if method in method_legend:
            #     legend_list.append(method_legend[method])
        ax_L.set_xlabel(self.x_label, fontsize=self.font_size)
        if not self.hide_ylabel:
            ax_L.set_ylabel(self.yL_label, fontsize=self.font_size)
        if not self.hide_yRlabel:
            ax_R.set_ylabel(self.yR_label, fontsize=self.font_size)

        if self.ylim:
            ytick       = list(np.linspace(self.ylim[0], self.ylim[1], self.y_major_num))
            ytick_minor = list(np.linspace(self.ylim[0], self.ylim[1], 0 if self.grid_minor is False else self.y_major_num * 2 - 1))
            ax_L.set_yticks(ytick)
            ax_L.set_yticks(ytick_minor, minor=True)
        if self.xlim:
            xtick       = list(np.linspace(self.xlim[0], self.xlim[1], self.x_major_num))
            xtick_minor = list(np.linspace(self.xlim[0], self.xlim[1], 0 if self.grid_minor is False else self.x_major_num * 2 - 1))
            ax.set_xticks(xtick)
            ax.set_xticks(xtick_minor, minor=True)

        if self.yL_tick:
            ax_L.set_yticks(self.yL_tick)
            ax_L.set_yticklabels(self.yL_tick, fontsize=self.tick_size)
        if self.yR_tick:
            ax_R.set_yticks(self.yR_tick)
            ax_R.set_yticklabels(self.yR_tick, fontsize=self.tick_size)
        if self.yfloat:
            ax_L.yaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        if self.x_tick:
            ax_L.set_xticks(self.x_tick)
            ax_L.set_xticklabels(self.x_tick, fontsize=self.tick_size)
        if self.yL_lim:
            ax_L.set_ylim(*self.yL_lim)
        if self.yR_lim:
            ax_R.set_ylim(*self.yR_lim)
        if self.x_lim:
            ax.set_xlim(*self.x_lim)
        ax_L.grid(color='#dbdbdb', **self.grid_type, which='both' if self.grid_minor else 'major', zorder=0)
        ax_L.legend(fontsize=self.legend_size, bbox_to_anchor=self.legendL_anchor, frameon=False, ncol=self.legend_ncol)
        ax_R.legend(fontsize=self.legend_size, bbox_to_anchor=self.legendR_anchor, frameon=False, ncol=self.legend_ncol)
        if self.aux_plt_func:
            self.aux_plt_func(ax_L, ax_R)
        plt.savefig(str(Path(self.pic_dir) / fig_name), format='pdf', bbox_inches='tight')
        plt.close()