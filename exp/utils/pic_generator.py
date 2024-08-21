from pathlib import Path
import json

from utils.pic_line_drawer import LineDrawer
from utils.pic_bar_drawer import BarDrawer
from utils.color_printer import print_OK


class PicGenerator(object):

    def __init__(self, data_path: str, style_path: str):
        self.__data_path = data_path
        self.__style_path = style_path
        self.__ld = LineDrawer(data_path)
        self.__bd = BarDrawer(data_path)
        self.__figs_type = {
            '03a'  : 'line_one_ax', '03b' : 'line_one_ax'  , '03c' : 'line_one_ax', '03d' : 'line_one_ax',
            '04a'  : 'line_one_ax', '04b' : 'line_one_ax'  , '04c' : 'line_one_ax',
            '12_la': 'line_one_ax', '12_a': 'line_one_ax'  , '12_b': 'line_one_ax', '12_c': 'line_one_ax', '12_d': 'line_one_ax', '12_e': 'line_one_ax',
            '13'   : 'bar_with_line',
            '14'   : 'bar_one_ax' , '15a' : 'bar_with_line', '15b': 'bar_with_line',
            '16'   : 'line_one_ax', '17'  : 'line_one_ax',
            '18a'  : 'line_one_ax', '18b' : 'line_one_ax'  , '18c': 'line_one_ax' , '18d': 'line_one_ax' , '18e': 'line_one_ax' , '18f': 'line_one_ax',
            '19a'  : 'line_two_ax', '19b': 'line_two_ax'   , '19c': 'line_two_ax'
        }
        self.__transmit_data = None


    def __plot_03a_goal(self, ax):
        ax.plot(10, 10.5, color='orange', marker='*', markersize=13)  # markerfacecolor='none'
        ax.text(16, 8.3, 'Goal', fontsize=13, color='orange', style='italic')

    def __plot_03b_bandwidth_bound(self, ax):
        ax.axhline(y=16, ls=(0, (5, 3)), c='#D63026', lw=1)
        ax.text(160, 20, 'Bandwidth-Bound', fontsize=12, color='#D63026')

    def __plot_03c_cache_bound(self, ax):
        ax.axhline(y=20, ls=(0, (5, 3)), c='#D63026', lw=1)
        ax.text(240, 24, 'Cache-Bound', fontsize=12, color='#D63026')

    def __plot_03d_example_line(self, ax):
        ax.axhline(y=90, ls=(0, (5, 3)), c='black', lw=1)
        ax.text(4.6, 92, '90%', fontsize=10, color='black')

    def __plot_04c_upperbound(self, ax):
        ax.axhline(y=64, ls=(0, (5, 3)), c='#D63026', lw=1, zorder=3000)
        ax.text(27, 66, 'IOPS-Bound', fontsize=12, color='#D63026')

    def __plot_14_notation(self, ax):
        ax.axhline(y=100 / 100, ls=(0, (5, 3)), c='#D63026', lw=1, zorder=3100)
        ax.text(-0.4, 120 / 100, '100 MB', fontsize=8, color='#D63026', zorder=3000, bbox=dict(facecolor='white', edgecolor='none', alpha=1.0, pad=1))
        ax.text(1.6, 200 / 100, 'Hotspot Buffer\n   (Optional)', fontsize=8.5, color='black', zorder=3000, bbox=dict(facecolor='white', edgecolor='black', alpha=1.0, pad=1.8, linewidth=0.5))
        ax.annotate('', xy=(2.7, 50 / 100), xytext=(2.8, 200 / 100), arrowprops=dict(arrowstyle="<-", linewidth=0.8), zorder=3100)

    def generate(self, exp_num: str):
        fig_name = f"fig_{exp_num}.pdf"
        fig_type = self.__figs_type[exp_num]

        # load data
        with (Path(self.__data_path) / f'fig_{exp_num}.json').open(mode='r') as f:
            data = json.load(f)
        # load style
        with (Path(self.__style_path) / f'fig_{exp_num}.json').open(mode='r') as f:
            custom_style = json.load(f)
        # load func
        if exp_num == '03a':
            custom_style['aux_plt_func'] = self.__plot_03a_goal
        if exp_num == '03b':
            custom_style['aux_plt_func'] = self.__plot_03b_bandwidth_bound
        if exp_num == '03c':
            custom_style['aux_plt_func'] = self.__plot_03c_cache_bound
        if exp_num == '03d':
            custom_style['aux_plt_func'] = self.__plot_03d_example_line
        if exp_num == '04c':
            custom_style['aux_plt_func'] = self.__plot_04c_upperbound
        if exp_num == '14':
            custom_style['aux_plt_func'] = self.__plot_14_notation

        # draw
        if fig_type == 'line_one_ax':
            self.__ld.plot_with_one_ax(data, fig_name, custom_style=custom_style)
        elif fig_type == 'line_two_ax':
            self.__ld.plot_with_two_ax(data, fig_name, custom_style=custom_style)
        elif fig_type == 'bar_one_ax':
            self.__bd.plot_with_one_ax(data, fig_name, custom_style=custom_style)
        elif fig_type == 'bar_two_ax':
            self.__bd.plot_with_two_ax(data, fig_name, custom_style=custom_style)
        elif fig_type == 'bar_with_line':
            self.__bd.plot_with_line(data, fig_name, custom_style=custom_style)

        print_OK(f"Draw {fig_name} done!")
