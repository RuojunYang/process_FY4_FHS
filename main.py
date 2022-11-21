import datetime
import os
from configparser import ConfigParser

import numpy as np
import pandas as pd
import netCDF4
import arrow


def process_file(path, each_file, output, name):
    """
    :param path: str，FHS nc files source path
    :param each_file: str，single FHS nc file name
    :param output: str，FHS csv files output path
    :param name: str，suffix of csv file
    """

    # read .nc file
    flnm = os.path.join(path, each_file)
    try:
        data = netCDF4.Dataset(flnm)
        # FHS variables
        FPT = data.variables['FPT'][:][5:]

        Pixel_No = []
        Pixel_Hot_Spot_NO = []
        Reliability = []
        Lat = []
        Long = []
        Pixel_Size = []
        Burned_Size = []
        FRP = []
        Intensity = []
        Land_Type = []
        Administrstive_area = []

        for i in range(len(FPT)):
            Pixel_No.append(FPT[i].split()[0])
            Pixel_Hot_Spot_NO.append(FPT[i].split()[1])
            Reliability.append(FPT[i].split()[2])
            Lat.append(FPT[i].split()[3])
            Long.append(FPT[i].split()[4])
            Pixel_Size.append(FPT[i].split()[5])
            Burned_Size.append(FPT[i].split()[6])
            FRP.append(FPT[i].split()[7])
            Intensity.append(FPT[i].split()[8])
            Land_Type.append(FPT[i].split()[9])
            Administrstive_area.append(FPT[i].split()[10])

        df = pd.DataFrame()
        df['latitude'] = np.array(Lat).astype(float)
        df['longitude'] = np.array(Long).astype(float)
        df['Pixel_No'] = np.array(Pixel_No).astype(int)
        df['Pixel_Hot_Spot_NO'] = np.array(Pixel_Hot_Spot_NO).astype(int)
        df['Reliability'] = np.array(Reliability).astype(int)
        df['Pixel_Size(km)'] = np.array(Pixel_Size).astype(float)
        df['Burned_Size(hm)'] = np.array(Burned_Size).astype(float)
        df['FRP(mw)'] = np.array(FRP).astype(float)
        df['Intensity'] = np.array(Intensity).astype(int)
        df['Land_Type'] = np.array(Land_Type).astype(int)
        df['Administrstive_area'] = np.array(Administrstive_area).astype(int)

        # no data, stop early
        if len(df) == 0:
            return
        # get time inside file name
        s = each_file.split('_')[-4]
        dt_str = s[:8] + ' ' + s[8:]
        # UTC to Beijing time
        date = arrow.get(dt_str).shift(hours=8)

        df['Beijing_Time'] = date.format('YYYY-MM-DD HH:mm:ss')
        # reformat the columns
        df = df[['Beijing_Time', 'latitude', 'longitude', 'Pixel_No', 'Pixel_Hot_Spot_NO', 'Reliability',
                 'Pixel_Size(km)', 'Burned_Size(hm)', 'FRP(mw)', 'Intensity', 'Land_Type',
                 'Administrstive_area']]
        # df = df.dropna()

        path_str = os.path.join(output, date.format('YYYY'), date.format('YYYYMM'), date.format('YYYYMMDD'))

        if not os.path.exists(path_str):
            os.makedirs(path_str)

        if os.path.isfile(
                os.path.join(path_str, name + '_' + date.format('YYYYMMDD') + str(date.hour).rjust(2, '0') + '.csv')):
            # read existing file
            temp = pd.read_csv(os.path.join(path_str, name + '_' + date.format('YYYYMMDD') +
                                            str(date.hour).rjust(2, '0') + '.csv'))
            temp = temp.drop_duplicates()
            # check if the timestamp already in file
            if date.format('YYYY-MM-DD HH:mm:ss') not in set(temp['Beijing_Time']):
                # if not, add into the file
                df = pd.concat([temp, df])
                df = df.drop_duplicates()
                df.to_csv(os.path.join(path_str,
                                       name + '_' + date.format('YYYYMMDD') + str(date.hour).rjust(2, '0') + '.csv'),
                          index=False)
                print('Update ' + os.path.join(path_str,
                                       name + '_' + date.format('YYYYMMDD') + str(date.hour).rjust(2, '0') + '.csv'))
        # if file not exist, directly write
        else:
            df.to_csv(
                os.path.join(path_str, name + '_' + date.format('YYYYMMDD') + str(date.hour).rjust(2, '0') + '.csv'),
                index=False)
            print('Create ' + os.path.join(path_str, name + '_' + date.format('YYYYMMDD') + str(date.hour).rjust(2, '0') + '.csv'))
    except Exception as e:
        print(e)


def process_folder(path, output):
    config = ConfigParser()
    config.read('./config.cfg')
    name = config.get('str', 'prefix')
    for each_file in os.listdir(path):
        process_file(path, each_file, output, name)


if __name__ == '__main__':
    config = ConfigParser()
    config.read('./config.cfg')
    process_folder(config.get('path', 'src'), config.get('path', 'out'))

