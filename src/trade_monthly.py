import numpy as np
import pandas as pd
import csv
import os


class BackTest():
    """implement backTest on A-Shares Stocks
    """

    def __init__(self, resPath, positionPath, hedge):
        """User can change settings there to conduct different back test over this trading logic
        """

        # settings
        self.shortCode = 500 # ic
        self.startP = '2010-12-31'
        self.start = '2011-01-04'  # start day
        self.end = '2022-06-30'  # end day
        self.CAPITAL = 10**7  # for each stock
        self.hedgeRatio = hedge
        self.resPath = resPath  # file path to save results
        self.positionPath = positionPath
        self.DAY = 242
        self.buyFee = 1/1000
        self.sellFee = 1/1000
        self.manageFee = 1/100/self.DAY
        # initiation
        self.asset = self.CAPITAL
        self.cash = self.asset
        self.short_holding = 0
        self.long_holding = 0
        self.long_fee = 0
        self.short_fee = 0
        self.longBill = pd.DataFrame()
        self.shortInfo = pd.Series(dtype='float')
        self.short_margin = 0
        self.longBool = False

        self.prepare_data()

    def prepare_data(self):
        """read data for calculation
        """
        # stock forward
        self.openPF = pd.read_csv(
            'data/openPF.csv').set_index('TradingDate')  # adjusted open price everyday
        self.openPF.fillna(method='ffill', inplace=True)

        # stock price
        self.openP = pd.read_csv(
            'data/openP.csv').set_index('Trddt')  # open price everyday
        self.openP.fillna(method='ffill', inplace=True)

        # future price
        self.openFuture = pd.read_csv('data/futureOpen.csv').set_index('Trddt')

        # risk free rate
        self.riskFree = pd.read_csv(
            'data/risk_free.csv').set_index('Clsdt')/100

        # circulating value everyday
        self.cValue = pd.read_csv('data/cValue.csv').set_index('Trddt')
        self.cValue.fillna(method='ffill', inplace=True)

        # set timeline
        # trading timeline
        self.timeline = list(self.cValue.loc[self.start:self.end].index)

        # set position
        self.longPosition = pd.read_excel(
            self.positionPath).set_index('trade_date')
        self.shortPosition = pd.read_csv(
            f'strategy/future_position_{self.shortCode}.csv').set_index('changeDate')

    def get_last_month(self,date):
        """calculate last month

        Args:
            date (str): '2011-01-04'

        Returns:
            str: '2010-12'
        """
        y = date[:4]
        m = int(date[5:7])-1
        if m==0:
            m=12
            y=str(int(y)-1)
        m=str(m).rjust(2,'0')
        return y+'-'+m

    def get_price(self, date, codes, type='f'):
        """get open price of a day

        Args:
            date (str): '2011-01-04'
            codes (list): ['1','21','600818']
            type (str, optional): type of price. Defaults to 'f'.
        Returns:
            dataFrame: open price
        """
        if type == 'f':  # 前复权股价
            return self.openPF.loc[date, codes]
        if type == 'n':  # 除权股价
            return self.openP.loc[date, codes]
        if type == 'future':  # 期货
            price = self.openFuture.loc[date, codes]
            if not price > 0:
                print(f'Fail to get future price of {codes} on {date}')
            return price

    def get_risk_free(self, date, type='d'):
        """get risk free rate of a day

        Args:
            date (str): '2011-01-04'
            type (str, optional): type of interest rate. Defaults to 'd'.

        Returns:
            _type_: _description_
        """
        if type == 'm':  # 月化
            return self.riskFree.loc[date, 'Nrrmtdt']
        if type == 'd':  # 日化
            return self.riskFree.loc[date, 'Nrrdaydt']

    def get_cValue(self, date, stks):
        """get circulating value of a day

        Args:
            date (str): '2011-01-04'
            stks (list): ['1','21','600818']

        Returns:
            dataFrame: circulating value
        """

        return self.cValue.loc[date, stks]

    def get_target_p(self, date):
        """get target portfolio of the next month

        Args:
            date (str): end day of a month: '2011-01-31'

        Returns:
            dataFrame: stocks and weights
        """
        try:
            pos = self.longPosition.loc[date]
        except:
            return []
        else:
            return pos[pos > 0]

    def calculate_pnl(self, date, yes):
        """calculate pnl for a day

        Args:
            date (str): today: '2011-01-05'
            yes (str): yesterday: '2011-01-04'
        """
        # 多头
        if not self.longBool:  # 空仓
            rf = self.get_risk_free(date, type='d')+1
            self.asset = self.asset*rf
            self.cash = self.cash * rf
            return
         # 非空仓
        oldBill = self.longBill[self.longBill['date'] == yes]
        newBill = oldBill.copy()
        newBill['date'] = date
        newBill['price'] = self.get_price(date, newBill.index)*100
        newBill['real'] = newBill['price'] * newBill['n']
        self.longBill = self.longBill.append(newBill)
        pnl = newBill['real'].sum() - oldBill['real'].sum()

        # update
        self.asset += pnl
        self.long_holding = newBill['real'].sum()

        # 空头

        # caluclate short pnl
        newPrice = self.get_price(
            date, self.shortInfo['Symbol'], type='future')*self.shortInfo['ContractMultiple']
        newShortHolding = newPrice*self.shortInfo['nContract']
        pnl = newShortHolding-self.short_holding

        # update
        self.asset -= pnl
        self.short_holding = newShortHolding
        self.short_margin = self.short_holding * \
            self.shortInfo['ShortTradingMargin']

    def change_long(self, date, p):
        """change long position

        Args:
            date (str): start day of a month: '2011-01-04'
            p (dataFrame): target position
        """
        # 下月有仓位
        if len(p):
            if self.longBool:  # 上月有仓位，调仓
                self.adjust_long(date, p)
            else:  # 上月无仓位，开仓
                self.long(date, p)
                self.short(date)
        else:  # 下月无仓位
            if self.longBool:  # 上月有仓位，平仓
                self.close_long(date)
                self.close_short(date)

    def adjust_long(self, date, p):
        """adjust long position

        Args:
            date (str): start day of a month: '2011-01-04'
            p (dataFrame): target position
        """
        oldBill = self.longBill[self.longBill['date'] == date]
        self.cash += oldBill['real'].sum()

        # new bill
        p = p*self.asset
        open = self.get_price(date, p.index, 'n')*100
        openF = self.get_price(date, p.index)*100

        newBill = pd.merge(p, open, left_index=True,
                           right_index=True).dropna()  # open and value
        newBill.columns = ['asset', 'open']

        # 计算仓位
        newBill['n'] = (newBill['asset']/newBill['open']
                        ).astype(int)  # 向下取整，真正手数
        newBill['real'] = newBill['open']*newBill['n']
        newBill = newBill[newBill['n'] != 0]
        newBill['price'] = openF  # 前复权价格
        newBill['n'] = newBill['real'] / newBill['price']  # 复权后手数
        newBill['date'] = date
        self.longBill = self.longBill[self.longBill['date'] != date]
        self.longBill = self.longBill.append(newBill[['price', 'n', 'real', 'date']])

        # fee

        tmp = pd.merge(oldBill['n'], newBill['n'],
                       right_index=True, left_index=True, how='outer')
        tmp = tmp.fillna(0)
        diff = tmp['n_y']-tmp['n_x']

        sell = diff[diff < 0].drop_duplicates()
        buy = diff[diff > 0].drop_duplicates()
        fee = -(oldBill.loc[sell.index, 'price'] * sell).sum()*self.sellFee
        fee += (newBill.loc[buy.index, 'price'] * buy).sum()*self.buyFee

        # update
        self.long_fee += fee
        self.long_holding = newBill['real'].sum()
        self.cash -= self.long_holding

    def long(self, date, p):
        """open long position for stocks

        Args:
            date (date): trading date: '2011-01-04'
            p (Series): target portfolio,index: code; value: weight
        """
        p = p*self.asset
        open = self.get_price(date, p.index, 'n')*100
        openF = self.get_price(date, p.index)*100

        bill = pd.merge(p, open, left_index=True,
                        right_index=True).dropna()  # open and value
        bill.columns = ['asset', 'open']
        bill['n'] = (bill['asset']/bill['open']).astype(int)  # 向下取整
        bill['real'] = bill['open']*bill['n']
        bill = bill[bill['n'] != 0]  # 去掉0
        bill['price'] = openF
        bill['n'] = bill['real'] / bill['price']
        bill['date'] = date
        self.longBill = self.longBill.append(bill[['price', 'n', 'real', 'date']])

        # fee
        bill['fee'] = bill['real']*self.buyFee
        bill.loc[bill['fee'] < 5, 'fee'] = 5

        # udpate
        self.long_fee += bill['fee'].sum()
        self.long_holding = bill['real'].sum()
        self.cash -= self.long_holding
        self.longBool = True

    def close_long(self, date):
        """close long position

        Args:
            date (str): start day of a month: '2011-01-04'
        """
        oldBill = self.longBill[self.longBill['date'] == date]

        self.long_holding = 0
        self.cash += oldBill['real'].sum()
        self.long_fee += oldBill['real'].sum()*self.sellFee
        self.longBill = self.longBill[self.longBill['date'] != date]
        self.longBool = False

    def short(self, date):
        """open short position 

        Args:
            date (str): 3 days before the last trading day of the future: '2011-01-18'
        """
        # get short position
        info = self.shortPosition[self.shortPosition.index <=
                                  date].iloc[-1].copy()
        price = self.get_price(
            date, info['Symbol'], type='future')*info['ContractMultiple']

        capital = self.long_holding*self.hedgeRatio  # short capital
        info['nContract'] = int(capital/price)
        self.short_holding = info['nContract']*price
        info['shortPrice'] = price
        self.short_margin = self.short_holding*info['ShortTradingMargin']

        # update
        self.short_fee += self.short_holding*info['TradingFee']
        self.shortInfo = info.copy()

    def close_short(self, date):
        """close short position and settle down

        Args:
            date (str): 3 days before the last trading day of the future: '2011-01-18'
        """
        # sell old position
        info = self.shortInfo.copy()
        closeShortP = self.get_price(
            date, info['Symbol'], type='future')*info['ContractMultiple']
        settle = (info['shortPrice']-closeShortP)*info['nContract']
        self.cash += settle
        self.short_holding = 0
        self.short_margin = 0
        self.short_fee += info['nContract'] * \
            closeShortP*info['TradingFee']  # sell fee
        self.shortInfo = pd.Series(dtype='float')

    def adj_short(self, date):
        """adj short position and settle down

        Args:
            date (str): 
        """
        # sell old position
        info = self.shortInfo.copy()
        adjShortP = self.get_price(
            date, info['Symbol'], type='future')*info['ContractMultiple']
        capital = self.long_holding*self.hedgeRatio  # short capital
        adjN = int(capital/adjShortP)
        if adjN==info['nContract']:
            return
        elif adjN > info['nContract']:
            changeN = adjN-info['nContract']
        else:
            changeN = info['nContract'] - adjN

        info['nContract'] = adjN

        # settle = (info['shortPrice']-adjShortP)*info['nContract']
        # self.cash += settle

        # update
        self.short_holding = info['nContract']*adjShortP
        self.short_margin = self.short_holding*info['ShortTradingMargin']
        self.short_fee += changeN*adjShortP*info['TradingFee']
        self.shortInfo = info.copy()

    def record(self, date=None, start=False):
        """record today's asset

        Args:
            date (str, optional): '2011-01-04'. Defaults to None.
            start (bool, optional): True: first day, False: Not the first day. Defaults to False.
        """
        if start:
            with open(f'{self.resPath}/asset.csv',
                      'w',
                      encoding='utf-8',
                      newline='') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(
                    ['date', 'total_asset', 'cash', 'long_holding', 'short_holding', 'short_margin', 'long_fee', 'short_fee'])
        else:
            with open(f'{self.resPath}/asset.csv',
                      'a',
                      encoding='utf-8',
                      newline='') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(
                    [date, self.asset, self.cash, self.long_holding, self.short_holding, self.short_margin, self.long_fee, self.short_fee])

    def act_start_day(self):
        """initialize for the first day of the back-test
        """
        self.change_long(self.start, self.get_target_p(self.startP))
        self.nextShortIdx = 1
        self.nextShortDay = self.shortPosition.index[self.nextShortIdx]

    def act_end_day(self):
        """close position for the last day of back-test
        """
        if self.longBool:
            self.close_short(self.end)
            self.close_long(self.end)

    def act_adjust_short(self, date):
        """adjust short position

        Args:
            date (date): trading date: '2011-01-04'
        """
        # 移仓换月
        if date == self.nextShortDay:
            # 换空头仓
            if self.longBool:
                self.close_short(date)
                self.short(date)
            # 设置下一个移仓换月日
            self.nextShortIdx += 1
            if len(self.shortPosition) == self.nextShortIdx:  # 避免最后一天bug
                self.nextShortDay = -1
            else:
                self.nextShortDay = self.shortPosition.index[self.nextShortIdx]
        # 检查对冲比例
        else:
            if self.longBool:
                self.adj_short(date)

    def act_end_everyday(self, date):
        """end work for every day

        Args:
            date (str): trading date
        """
        self.long_fee += self.long_holding*self.manageFee  # 每日管理费
        self.asset -= (self.long_fee+self.short_fee)
        self.cash -= (self.long_fee+self.short_fee)
        # 记录今日
        self.record(date)

    def act_ini_everyday(self):
        """initialize a trading day
        """
        self.long_fee = 0
        self.short_fee = 0

    def run(self):
        """travel through everyday to calculate profit and loss
        """

        self.record(start=True)
        flag = False  # 是否准备换仓
        for i in range(len(self.timeline)):
            today = self.timeline[i]
            self.act_ini_everyday()

            # 开仓
            if today == self.start:
                self.act_start_day()

            # 非开仓日
            else:
                # 标记时间
                yes = self.timeline[i-1]  # 昨天
                if i != len(self.timeline)-1:
                    tom = self.timeline[i+1]  # 明天
                month = today[:7]

                # 算pnl
                self.calculate_pnl(today, yes)

                # 平仓日
                if today == self.end:
                    self.act_end_day()

                else:  # 非第一天or最后一天
                    # 换多头仓
                    if flag:
                        self.change_long(today, self.get_target_p(yes))
                        flag = False

                    # adjust short
                    self.act_adjust_short(today)

                    # 月底
                    if month[5:] != tom[5:7]:  # 准备换多头仓
                        flag = True

            self.act_end_everyday(today)

        self.longBill.index.name = 'stock'
        self.longBill.to_csv(f'{self.resPath}/stock_records.csv')


if __name__ == '__main__':
    ins = BackTest('res_tmp','strategy_sector_neutral/strategy_senti_sn.xlsx',0.25)
    ins.run()  # run
