import pandas as pd
import os
import glob
import time
from datetime import datetime
from shared_mixin import SharedMixin


class BlockRateAnalyser(SharedMixin):

    def __init__(self, node_data_dir):
        self.supernode_data = {
            self.get_supernode_from_path(csv_path): pd.read_csv(csv_path)
            for csv_path in glob.iglob(f"{node_data_dir}/*.csv")
        }

        self.analysis_periods = [12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0.5]
        self.analysis_periods_str = [str(n) for n in self.analysis_periods]
        self.constant_period_samples = 24
        self.constant_hour = 0.5  # hours

        self.max_csv_rows = 10000
        self.rate_csv_dir = "node_rate"
        if not os.path.exists(self.rate_csv_dir):
            os.mkdir(self.rate_csv_dir)

        super().__init__()

    def get_increasing_period_rates(self, rate_df, chosen_datetime=None):
        for supernode, df in self.supernode_data.items():
            df["timestamp"] = df["datetime"].apply(
                lambda x: time.mktime(datetime.strptime(x, self.datetime_format).timetuple()))

            latest_timestamp = time.mktime(datetime.strptime(chosen_datetime, self.datetime_format).timetuple()) \
                if chosen_datetime else df.iloc[-1].timestamp
            supernode_rate = {}
            for hours in self.analysis_periods:
                seconds = hours * 3600
                target_timestamp = latest_timestamp - seconds
                self.update_supernode_rate(target_timestamp, latest_timestamp, df, supernode_rate, str(hours), hours)

            supernode_result = pd.DataFrame(supernode_rate, index=[supernode])
            rate_df = rate_df.append(supernode_result)

        return rate_df

    def get_constant_period_rates(self, rate_df, chosen_datetime=None):
        for supernode, df in self.supernode_data.items():
            df["timestamp"] = df["datetime"].apply(
                lambda x: time.mktime(datetime.strptime(x, self.datetime_format).timetuple()))

            end_timestamp = time.mktime(datetime.strptime(chosen_datetime, self.datetime_format).timetuple()) \
                if chosen_datetime else df.iloc[-1].timestamp
            supernode_rate = {}
            for i in range(self.constant_period_samples):
                seconds = self.constant_hour * 3600
                start_timestamp = end_timestamp - seconds
                self.update_supernode_rate(start_timestamp, end_timestamp, df, supernode_rate, (i+1)*self.constant_hour, self.constant_hour)
                end_timestamp -= self.constant_hour * 3600

            supernode_result = pd.DataFrame(supernode_rate, index=[supernode])
            rate_df = rate_df.append(supernode_result)

        return rate_df

    @staticmethod
    def update_supernode_rate(from_timestamp, to_timestamp, df, supernode_rate, tag, hours):
        from_sr = df[df.timestamp <= from_timestamp]
        if not from_sr.empty:
            from_sr = from_sr.sort_values(by=["timestamp"], ascending=False).iloc[0]
        to_sr = df[df.timestamp >= to_timestamp]
        if not to_sr.empty:
            to_sr = to_sr.sort_values(by=["timestamp"], ascending=True).iloc[0]

        if not from_sr.empty and not to_sr.empty:
            average_votes = (to_sr.vote_amount + from_sr.vote_amount) / 2
            supernode_rate[tag] = round(
                ((to_sr.block_amount - from_sr.block_amount) / average_votes) * 10000000 / hours, 3)  # increase in block per 10M votes per hour
        return supernode_rate

    def save_rates_to_csv(self, rate_df):
        for i, row in rate_df.iterrows():
            path = f"{self.rate_csv_dir}/{i}.csv"
            if not os.path.exists(path):
                f = open(path, "w")
                f.close()
                csv_df = pd.DataFrame(columns=["datetime", *self.analysis_periods_str])
            else:
                csv_df = pd.read_csv(path)
            csv_sr = pd.Series(self.datetime_now, index=["datetime"])
            csv_sr = csv_sr.append(row)
            csv_df = csv_df.append(csv_sr, ignore_index=True)[-self.max_csv_rows:]
            csv_df.to_csv(path, index=False)

    @staticmethod
    def sort_supernode_by_rate(rate_df, columns, label_postfix=""):
        sorted_supernodes = pd.DataFrame()
        for col in columns:
            sorted_sr = rate_df.sort_values(by=col, ascending=False)[col]
            sorted_supernodes[str(col) + label_postfix] = [i + " (" + str(rate) + ")" for i, rate in sorted_sr.iteritems()]  # + " hr (block rate per 10M votes per hour)"
        return sorted_supernodes

    def run(self):
        increasing_period_rate_df = self.get_increasing_period_rates(pd.DataFrame(columns=self.analysis_periods_str))
        # self.save_rates_to_csv(rate_df)
        sorted_increasing_period_supernodes = self.sort_supernode_by_rate(increasing_period_rate_df, self.analysis_periods_str, " hr earlier")

        constant_period_rate_df = self.get_constant_period_rates(
            pd.DataFrame(columns=list(reversed(range(1, self.constant_period_samples)))))
        sorted_constant_period_supercodes = self.sort_supernode_by_rate(constant_period_rate_df, constant_period_rate_df.columns, " hr point")
        a = 1


if __name__ == "__main__":
    analyser = BlockRateAnalyser("node_data")
    analyser.run()
