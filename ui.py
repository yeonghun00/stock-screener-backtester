from PyQt5 import QtWidgets, uic, QtCore
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar

import screener
from util import downloader, formatter

from matplotlib import pyplot as plt
from datetime import datetime
import pandas as pd
import numpy as np
from scipy.stats.mstats import gmean
import util.reference_file as refer
import yfinance as yf
import json
import os

tickers = ['AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CSCO', 'CVX', 'GS', 'HD', 'HON', 'IBM', 'INTC', 'JNJ', 'KO', 'JPM',
       'MCD', 'MMM', 'MRK', 'MSFT', 'NKE', 'PG', 'TRV', 'UNH', 'CRM', 'VZ', 'V', 'WBA', 'WMT', 'DIS', 'DOW']


# MDD 샤프비율 지우고 새로운거 

class Ui_option(QtWidgets.QMainWindow):
    def __init__(self):
        super(Ui_option, self).__init__()
        uic.loadUi('option.ui', self)

        self.pushButton_update.clicked.connect(self.update_event)
        self.pushButton_download.clicked.connect(self.download_event)
        self.pushButton_back.clicked.connect(self.back_event)
        self.textEdit.setReadOnly(True)

    def update_event(self):
        self.index_path = os.getcwd() + '/data/nasdaq.pkl'
        nasdaq = yf.Ticker("^IXIC").history(period="max")
        dates = pd.date_range(nasdaq.index[0], nasdaq.index[-1])
        nasdaq = pd.DataFrame(nasdaq, index=dates).tz_localize('UTC').ffill()
        nasdaq.to_pickle(self.index_path)
        self.textEdit.append('Updated')

    def download_event(self):
        s = self.dateEdit_start.date().toString('yyyy-MM-dd')
        e = self.dateEdit_end.date().toString('yyyy-MM-dd')

        if self.checkBox_overwrite.isChecked():
            for path in [os.getcwd() + '/data/' + str(x) + '/' for x in range(int(s[:4]),int(e[:4])+1)]:
                os.remove(path)

        dates = pd.date_range(s, e, freq='Ys').format(formatter=lambda x: x.strftime('%Y/%m/%d'))
        for i in range(len(dates) - 1):
            self.textEdit.append('{} {}'.format(dates[:-1][i], dates[1:][i]))
            d = downloader.Download(dates[:-1][i], dates[1:][i])
            # tickers = d.get_tiingo_supported(s,e)['ticker']
            with downloader.Pool(30) as pool:
                pool.map(d.download, tickers)
            print(dates[:-1][i], dates[1:][i])
        print('Download FINISHED')

        try:
            print('FORMAT')
            for i in range(int(s[:4]), int(e[:4])+1):
                f = formatter.Format(str(i))
                print('complete', i)
            print('Format FINISHED')
            self.textEdit.setText('Downloaded')
        except Exception as e:
            print(e)

    def back_event(self):
        self.close()

class Ui(QtWidgets.QMainWindow):
    def __init__(self):
        super(Ui, self).__init__()
        uic.loadUi('backtester.ui', self)

        # windows
        self.pushButton_option.clicked.connect(self.option_event)

        # screener
        self.comboBox_file.activated[str].connect(lambda: self.change_file_event(self.comboBox_file))
        self.comboBox_column.activated[str].connect(self.set_label_condition_event)
        self.pushButton_today.clicked.connect(self.today_event)
        self.pushButton_remove_condition.clicked.connect(self.remove_equation_event)
        self.pushButton_edit_condition.clicked.connect(self.edit_equation_event)
        self.pushButton_add_condition.clicked.connect(self.add_equation_event)
        self.pushButton_new_screen.clicked.connect(self.new_screen_event)
        self.pushButton_load_screen.clicked.connect(self.load_screen_event)
        self.listWidget_saved_screens.itemDoubleClicked.connect(self.load_screen_event)
        self.pushButton_save_screen.clicked.connect(self.save_screen_event)
        self.pushButton_save_as_screen.clicked.connect(self.save_as_screen_event)
        self.pushButton_screen.clicked.connect(self.screen_event)
        self.pushButton_remove_screen.clicked.connect(self.remove_screen_event)
        self.pushButton_rename_screen.clicked.connect(self.rename_screen_event)
        self.listWidget_conditions.itemClicked.connect(self.set_comboBox_event)
        self.listWidget_screened_stocks.itemClicked.connect(self.show_stocks_event)
        self.dateEdit_screen.setDate(QtCore.QDate.currentDate())
        self.dateEdit_screen.dateChanged.connect(self.date_change_event)

        # stock 1
        self.fig = plt.Figure()
        self.canvas = FigureCanvas(self.fig)
        # self.toolbar = NavigationToolbar(self.canvas, self)
        self.verticalLayout_stock.addWidget(self.canvas)
        # self.verticalLayout_stock.addWidget(self.toolbar)

        self.quarter_period = ['Quarter', 'Annual']
        self.quarter_growth_period = ['Recent quarter', 'Recent annual', 'Recent year quarter']
        self.conditions_dic = {}
        self.condition_id = 0

        self.saved_screen = ''
        self.setup_screen_manager()
        self.current_screen_name = ''
        self.screen_date = datetime.today().strftime('%Y-%m-%d') #'2021-11-01'

        # path settings
        data_path = os.getcwd() + '/data/' + self.screen_date[:4] + '/'
        self.path_dic = {
            'raw': data_path + 'raw/',
            'daily': data_path + 'formatted/daily_fund/',
            'quarter': data_path + 'formatted/quarter_fund/quarter/',
            'price': data_path + 'formatted/price/'
        }

        # backtester
        self.pushButton_reset.clicked.connect(self.reset_backtest_event)
        self.pushButton_backtest.clicked.connect(self.backtest_event)
        self.tableWidget_performance.itemClicked.connect(self.set_comboBox_event)

        header = self.tableWidget_performance.horizontalHeader()
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
        header.setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QtWidgets.QHeaderView.Stretch)
        header.setSectionResizeMode(4, QtWidgets.QHeaderView.Stretch)
        header.setSectionResizeMode(5, QtWidgets.QHeaderView.Stretch)

        # chart
        self.fig2 = plt.Figure()
        self.canvas2 = FigureCanvas(self.fig2)
        # self.toolbar = NavigationToolbar(self.canvas2, self)
        self.verticalLayout_performance.addWidget(self.canvas2)
        # self.verticalLayout_performance.addWidget(self.toolbar)

        self.performance_dic = {}
        self.tableWidget_performance.cellClicked.connect(self.show_stocks_event_backtest)
        self.show()

    def option_event(self):
        self.dialog = Ui_option()
        self.dialog.show()

    def setup_screen_manager(self):
        self.setup_json()
        self.listWidget_saved_screens.clear()
        for i in list(self.saved_screen):
            item = QtWidgets.QListWidgetItem()
            item.setText(QtWidgets.QApplication.translate("Dialog", i))
            self.listWidget_saved_screens.addItem(item)

    def setup_json(self):
        if not os.path.exists('saved.json'):
            with open('saved.json', 'w') as f:
                json.dump({}, f)
        with open('saved.json', 'r') as f:
            self.saved_screen = json.load(f)

    def get_checked_conditions(self):
        checked = []
        for index in range(self.listWidget_conditions.count()):
            if self.listWidget_conditions.item(index).checkState() == 2:
                checked.append(self.listWidget_conditions.item(index).text())
        checked = [int(x[:x.find('.')]) for x in checked]

        conditions1, conditions2, conditions3 = {}, {}, {}
        for k in {k: self.conditions_dic[k] for k in self.conditions_dic if k in checked}:
            if self.conditions_dic[k][0] == 'Daily':
                conditions1[self.conditions_dic[k][1]] = [self.conditions_dic[k][2], self.conditions_dic[k][3],
                                                          self.conditions_dic[k][4], self.conditions_dic[k][5]]
            elif self.conditions_dic[k][0] == 'Quarter':
                conditions2[self.conditions_dic[k][1]] = [self.conditions_dic[k][2], self.conditions_dic[k][3],
                                                          self.conditions_dic[k][4], self.conditions_dic[k][5],
                                                          self.conditions_dic[k][6]]
            elif self.conditions_dic[k][0] == 'Quarter Growth':
                conditions3[self.conditions_dic[k][1]] = [self.conditions_dic[k][2], self.conditions_dic[k][3],
                                                          self.conditions_dic[k][4], self.conditions_dic[k][5],
                                                          self.conditions_dic[k][6], self.conditions_dic[k][7]]

        return {
            'conditions1': conditions1,
            'conditions2': conditions2,
            'conditions3': conditions3,
        }

    def change_file_event(self, text):
        if text.currentText() == 'Daily':
            files = set(f[:f.find('.')] for f in os.listdir(self.path_dic['daily']) if 'pkl' in f)
            self.comboBox_period.setEnabled(False)
            self.comboBox_period_2.setEnabled(False)
        elif text.currentText() == 'Quarter':
            files = set(f[:f.find('.')] for f in os.listdir(self.path_dic['quarter']) if 'pkl' in f)
            self.comboBox_period.setEnabled(True)
            self.comboBox_period_2.setEnabled(False)

            self.comboBox_period.clear()
            self.comboBox_period.addItems(self.quarter_period)

        elif text.currentText() == 'Quarter Growth':
            files = set(f[:f.find('.')] for f in os.listdir(self.path_dic['quarter']) if 'pkl' in f)
            self.comboBox_period.setEnabled(True)
            self.comboBox_period_2.setEnabled(True)

            self.comboBox_period.clear()
            self.comboBox_period.addItems(self.quarter_growth_period)

        self.comboBox_column.clear()
        self.comboBox_column.addItems(files)

    def set_label_condition_event(self, text):
        self.label_condition.setText(text)
        self.label_condition.setToolTip(str(refer.r[[x['dataCode'] for x in refer.r].index(text)]).replace(',', "\n"))

    def set_comboBox_event(self):
        listItems = self.listWidget_conditions.selectedItems()
        if not listItems: return

        for item in listItems:
            print(item.text().split())
            self.comboBox_file.setCurrentText(item.text()[item.text().find('. ')+2:item.text().find(' -')])
            self.change_file_event(self.comboBox_file)
            self.comboBox_column.setCurrentText(item.text()[item.text().find('- ')+2:item.text().find(' |')])
            self.label_condition.setText(item.text()[item.text().find('- ')+2:item.text().find(' |')])

    def add_equation_event(self):
        try:
            self.conditions_dic[self.condition_id] = [self.comboBox_file.currentText(),
                                                      self.label_condition.text() + '_' + str(self.condition_id),
                                                      float(eval(self.lineEdit_condition.text())),
                                                      float(eval(self.lineEdit_condition_2.text())),
                                                      self.comboBox_equation.currentText(),
                                                      self.comboBox_equation_2.currentText(),
                                                      self.comboBox_period.currentText(),
                                                      self.comboBox_period_2.currentText()]

            if self.comboBox_period_2.isEnabled():
                text = '{}. {} - {} | {} {} {} x {} {} {}'.format(self.condition_id, self.comboBox_file.currentText(),
                                                                  self.label_condition.text(),
                                                                  self.comboBox_period.currentText(),
                                                                  float(eval(self.lineEdit_condition.text())),
                                                                  self.comboBox_equation.currentText(),
                                                                  self.comboBox_equation_2.currentText(),
                                                                  self.comboBox_period_2.currentText(),
                                                                  float(eval(self.lineEdit_condition_2.text())))

            elif self.comboBox_period.isEnabled():
                text = '{}. {} - {} | {} {} {} x {} {}'.format(self.condition_id, self.comboBox_file.currentText(),
                                                               self.label_condition.text(),
                                                               self.comboBox_period.currentText(),
                                                               float(eval(self.lineEdit_condition.text())),
                                                               self.comboBox_equation.currentText(),
                                                               self.comboBox_equation_2.currentText(),
                                                               float(eval(self.lineEdit_condition_2.text())))

            else:
                text = '{}. {} - {} | {} {} x {} {}'.format(self.condition_id, self.comboBox_file.currentText(),
                                                            self.label_condition.text(),
                                                            float(eval(self.lineEdit_condition.text())),
                                                            self.comboBox_equation.currentText(),
                                                            self.comboBox_equation_2.currentText(),
                                                            float(eval(self.lineEdit_condition_2.text())))
            self.condition_id += 1
        except:
            print('string in equation')
            return

        item = QtWidgets.QListWidgetItem()
        item.setText(QtWidgets.QApplication.translate("Dialog", text))
        item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable)
        item.setCheckState(2)
        self.listWidget_conditions.addItem(item)

    def edit_equation_event(self):
        listItems = self.listWidget_conditions.selectedItems()
        if not listItems:
            return
        for item in listItems:
            id = int(item.text()[:item.text().find('.')])
            self.conditions_dic.pop(int(item.text()[:item.text().find('.')]))
            try:
                # 수정
                self.conditions_dic[id] = [self.comboBox_file.currentText(),
                                           self.label_condition.text() + '_' + str(id),
                                           float(eval(self.lineEdit_condition.text())),
                                           float(eval(self.lineEdit_condition_2.text())),
                                           self.comboBox_equation.currentText(),
                                           self.comboBox_equation_2.currentText(),
                                           self.comboBox_period.currentText(),
                                           self.comboBox_period_2.currentText()]
                if self.comboBox_period_2.isEnabled():
                    text = '{}. {} - {} | {} {} {} x {} {} {}'.format(id, self.comboBox_file.currentText(),
                                                                      self.label_condition.text(),
                                                                      self.comboBox_period.currentText(),
                                                                      float(eval(self.lineEdit_condition.text())),
                                                                      self.comboBox_equation.currentText(),
                                                                      self.comboBox_equation_2.currentText(),
                                                                      self.comboBox_period_2.currentText(),
                                                                      float(eval(self.lineEdit_condition_2.text())))

                elif self.comboBox_period.isEnabled():
                    text = '{}. {} - {} | {} {} {} x {} {}'.format(id, self.comboBox_file.currentText(),
                                                                   self.label_condition.text(),
                                                                   self.comboBox_period.currentText(),
                                                                   float(eval(self.lineEdit_condition.text())),
                                                                   self.comboBox_equation.currentText(),
                                                                   self.comboBox_equation_2.currentText(),
                                                                   float(eval(self.lineEdit_condition_2.text())))

                else:
                    text = '{}. {} - {} | {} {} x {} {}'.format(id, self.comboBox_file.currentText(),
                                                                self.label_condition.text(),
                                                                float(eval(self.lineEdit_condition.text())),
                                                                self.comboBox_equation.currentText(),
                                                                self.comboBox_equation_2.currentText(),
                                                                float(eval(self.lineEdit_condition_2.text())))
                item.setText(text)

            except:
                print('string in equation')
                return

    def remove_equation_event(self):
        listItems = self.listWidget_conditions.selectedItems()
        if not listItems:
            return
        for item in listItems:
            self.conditions_dic.pop(int(item.text()[:item.text().find('.')]))
            self.listWidget_conditions.takeItem(self.listWidget_conditions.row(item))

    def screen_event(self):
        conditions = self.get_checked_conditions()
        s = screener.Screener(conditions, pd.to_datetime(self.screen_date).tz_localize('UTC'))
        screened_stocks = s.screen()
        self.label_stock_num.setText(str(len(screened_stocks)))
        self.listWidget_screened_stocks.clear()
        self.listWidget_screened_stocks.addItems(screened_stocks)

    def date_change_event(self, qDate):
        self.screen_date = ('{0}/{1}/{2}'.format(qDate.year(), qDate.month(), qDate.day()))

    def save_screen_event(self):
        conditions = self.get_checked_conditions()

        if self.listWidget_conditions.count() == 0:
            msg = QtWidgets.QMessageBox()
            msg.setWindowTitle("Warning")
            msg.setText("No condition")
            msg.setIcon(QtWidgets.QMessageBox.Warning)
            msg.exec_()  # this will show our messagebox
            return

        if self.label_screen_name.text() != '':
            self.saved_screen.update({self.label_screen_name.text(): conditions})
            with open('saved.json', 'w') as f:
                json.dump(self.saved_screen, f)
        else:
            text, okPressed = QtWidgets.QInputDialog.getText(self, "Save", "Name:", QtWidgets.QLineEdit.Normal, "")
            if okPressed and text != '':
                if text in self.saved_screen:
                    msg = QtWidgets.QMessageBox()
                    msg.setWindowTitle("Warning")
                    msg.setText(text + " name already exists")
                    msg.setIcon(QtWidgets.QMessageBox.Warning)
                    msg.exec_()  # this will show our messagebox
                    return
                self.label_screen_name.setText(text)
                self.label_backtest_name.setText(text)
                self.saved_screen.update({text:conditions})
                with open('saved.json', 'w') as f:
                    json.dump(self.saved_screen, f)
                self.setup_screen_manager()

    def save_as_screen_event(self):
        conditions = self.get_checked_conditions()
        text, okPressed = QtWidgets.QInputDialog.getText(self, "Save as", "Name:", QtWidgets.QLineEdit.Normal, "")
        if okPressed and text != '':
            self.label_screen_name.setText(text)
            self.saved_screen.update({text: conditions})
            with open('saved.json', 'w') as f:
                json.dump(self.saved_screen, f)
            self.setup_screen_manager()

    # Todos
    '''    
    self.comboBox_file.setCurrentText(
    '''
    def load_screen_event(self):
        self.new_screen_event()
        listItems = self.listWidget_saved_screens.selectedItems()
        if not listItems:
            return
        for item in listItems:
            screen = self.saved_screen[item.text()]
            self.label_screen_name.setText(item.text())
            self.label_backtest_name.setText(item.text())
            for c in screen:
                for element in screen[c]:
                    self.comboBox_period.setEnabled(False)
                    self.comboBox_period_2.setEnabled(False)
                    current = screen[c]
                    self.comboBox_file.setCurrentText('Daily')
                    if c == 'conditions2':
                        self.comboBox_file.setCurrentText('Quarter')
                        self.comboBox_period.setEnabled(True)
                        self.comboBox_period.setCurrentText(current[element][4])
                    elif c == 'conditions3':
                        self.comboBox_file.setCurrentText('Quarter Growth')
                        self.comboBox_period_2.setEnabled(True)
                        self.comboBox_period.setCurrentText(current[element][4])
                        self.comboBox_period_2.setCurrentText(current[element][5])
                    self.change_file_event(self.comboBox_file)
                    self.comboBox_column.setCurrentText(element[:element.find('_')])
                    self.label_condition.setText(element[:element.find('_')])
                    self.lineEdit_condition.setText(str(current[element][0]))
                    self.lineEdit_condition_2.setText(str(current[element][1]))
                    self.comboBox_equation.setCurrentText(current[element][2])
                    self.comboBox_equation_2.setCurrentText(current[element][3])
                    self.add_equation_event()

    def rename_screen_event(self):
        listItems = self.listWidget_saved_screens.selectedItems()
        if not listItems:
            return
        for item in listItems:
            text, okPressed = QtWidgets.QInputDialog.getText(self, "Save", "Name:", QtWidgets.QLineEdit.Normal, "")
            if okPressed:
                self.saved_screen[text] = self.saved_screen.pop(item.text())
                with open('saved.json', 'w') as f:
                    json.dump(self.saved_screen, f)
        self.setup_screen_manager()

    def remove_screen_event(self):
        listItems = self.listWidget_saved_screens.selectedItems()
        if not listItems:
            return
        for item in listItems:
            msg = QtWidgets.QMessageBox()
            msg.setWindowTitle("Warning")
            msg.setText('Remove ' + item.text() + '?')
            #msg.setInformativeText(str(self.saved_screen[item.text()]))
            msg.setIcon(QtWidgets.QMessageBox.Warning)
            msg.setStandardButtons(QtWidgets.QMessageBox.Cancel | QtWidgets.QMessageBox.Ok)
            m = msg.exec()
            if m == QtWidgets.QMessageBox.Cancel:
                return
            elif m == QtWidgets.QMessageBox.Ok:
                self.saved_screen.pop(item.text())
                with open('saved.json', 'w') as f:
                    json.dump(self.saved_screen, f)
                self.listWidget_saved_screens.takeItem(self.listWidget_saved_screens.row(item))
                self.setup_screen_manager()

    def new_screen_event(self):
        listItems = self.listWidget_conditions.selectedItems()

        if not listItems and self.label_screen_name == '':
            msg = QtWidgets.QMessageBox()
            msg.setWindowTitle("Warning")
            msg.setText("Please save the current screener")
            msg.setIcon(QtWidgets.QMessageBox.Warning)
            msg.exec_()  # this will show our messagebox

        self.listWidget_conditions.clear()
        self.listWidget_screened_stocks.clear()
        self.label_screen_name.setText('')
        self.conditions_dic = {}
        self.condition_id = 0

    def today_event(self):
        self.dateEdit_screen.setDate(QtCore.QDate.currentDate())

    def show_stocks_event(self):
        listItems = self.listWidget_screened_stocks.selectedItems()
        if not listItems: return

        for item in listItems:
            # chart
            path = self.path_dic['raw'] + item.text() + '/price.pkl'
            df = pd.read_pickle(path)
            self.fig.clear()
            ax = self.fig.add_subplot(111)
            ax.plot(df.index, df['adjClose'])
            ax.grid()
            self.canvas.draw()

            # detail
            path = self.path_dic['raw'] + item.text() + '/meta.pkl'
            df = pd.read_pickle(path)
            self.plainTextEdit_stock_detail.clear()

            for i in ['name', 'ticker', 'exchangeCode', 'startDate', 'endDate', 'description']:
                text = '{} :\n{}'.format(i, df.loc[i].values[0])
                self.plainTextEdit_stock_detail.appendPlainText(text)
            # self.plainTextEdit_stock_detail.setPlainText(df.to_string())

    # backtester
    def show_stocks_event_backtest(self, row, col):
        self.listWidget_backtest_stocks.clear()
        self.listWidget_backtest_stocks.addItems(self.performance_dic['result'][row])

    def reset_backtest_event(self):
        self.label_backtest_name.setText('')
        self.listWidget_backtest_stocks.clear()
        self.tableWidget_performance.setRowCount(0)
        self.fig2.clear()

    def backtest_event(self):
        if self.label_backtest_name.text() == '':
            return
        self.tableWidget_performance.setRowCount(0)

        start = self.dateEdit_start.date().toPyDate()
        end = self.dateEdit_end.date().toPyDate()
        cycle = int(self.spinBox_cycle.text())
        hold = int(self.spinBox_hold.text())

        self.performance_dic = {'date': [], 'return': [], 'total': [], 'increased': [], 'market': [],'result': []}
        screen_dates = pd.date_range(start=start, end=end, freq=str(cycle)+'MS').tz_localize("UTC")

        # 날짜 잘 따라가기
        index_df = pd.read_pickle(os.getcwd() + '/data/nasdaq.pkl')
        conditions = self.get_checked_conditions()

        for date in screen_dates:
            s = screener.Screener(conditions, date)
            r, total, increased = s.evaluate(hold)
            if total == 0:
                r = 1
            self.performance_dic['date'].append(date)
            self.performance_dic['return'].append(r)
            self.performance_dic['total'].append(total)
            self.performance_dic['increased'].append(increased)
            self.performance_dic['result'].append(s.screened_codes)
            self.performance_dic['market'].append(index_df.loc[date+pd.DateOffset(months=hold), 'Close']/index_df.loc[date, 'Close'])

        # table
        for d in self.performance_dic['date']:

            rowPosition = self.tableWidget_performance.rowCount()
            self.tableWidget_performance.insertRow(rowPosition)

            index = self.performance_dic['date'].index(d)
            self.tableWidget_performance.setItem(rowPosition, 0, QtWidgets.QTableWidgetItem(str(d.strftime('%Y-%m-%d'))))
            self.tableWidget_performance.setItem(rowPosition, 1, QtWidgets.QTableWidgetItem(str(self.performance_dic['total'][index])))
            self.tableWidget_performance.setItem(rowPosition, 2, QtWidgets.QTableWidgetItem(str(self.performance_dic['increased'][index])))
            self.tableWidget_performance.setItem(rowPosition, 3, QtWidgets.QTableWidgetItem(str(round(self.performance_dic['return'][index], 3))))
            self.tableWidget_performance.setItem(rowPosition, 4, QtWidgets.QTableWidgetItem(str(round(self.performance_dic['market'][index], 3))))
            self.tableWidget_performance.setItem(rowPosition, 5, QtWidgets.QTableWidgetItem(str(round(self.performance_dic['return'][index] - self.performance_dic['market'][index], 3))))

        # chart
        self.fig2.clear()
        ax = self.fig2.add_subplot(111)
        ax.plot(self.performance_dic['date'], np.cumprod(self.performance_dic['return']))
        ax.plot(self.performance_dic['date'], np.cumprod(self.performance_dic['market']))
        ax.legend(['return', 'market'])
        ax.grid()
        self.canvas2.draw()

        # summary

        # riskfree = 3%
        rf = 1.03**(1/(12/hold))
        std = np.std(self.performance_dic['return'])
        g_mean = gmean(self.performance_dic['return'])
        cov = np.cov(self.performance_dic['return'], self.performance_dic['market'])
        beta = cov[0,1]/cov[1,1]
        sharpe_ratio = (g_mean-rf)/std
        treynor_ratio = (g_mean-rf)/beta

        self.label_geomean.setText(str(round(g_mean, 3)))
        self.label_std.setText(str(round(std, 3)))
        self.label_win_rate.setText(str(round(sum(self.performance_dic['increased'])/sum(self.performance_dic['total']), 3)))
        self.label_screend_mean.setText(str(round(np.mean(self.performance_dic['total']), 3)))
        self.label_sharpe.setText(str(round(sharpe_ratio, 3)))
        self.label_treynor.setText(str(round(treynor_ratio, 3)))

# 성과검증 다음 폴더로 넘어가
# 토탈 0개 검출 시 예외 처리. 스크리너 문제
# 홀드 싸이클보다 작으면 오류 처리 => 앞 날짜가 없어서 생긴 오류였음. 예외처리 해야할ㄹ듯
# 다운로드 api 키 어떻게 유저프렌드리하게 만들지 생각