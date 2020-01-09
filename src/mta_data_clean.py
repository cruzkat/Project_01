import sys
import path
from os import path
import pandas as pd
import numpy as np
import datetime as dt
import pickle

# Define a function that takes in a list of files, imports the data, cleans the data,
#   and ranks the data by total traffic.
def mta_data_clean(data_path, file_list) :
    """

    data_path - path to directory for data files
    file_list - list of files that contain MTA turnstile data that should be 
                processed into the the dataset

    """


    # For each file in the list
    for i in range(len(file_list)) :

        dat_file = file_list[i]

        # check that the file exists
        if(not path.exists(data_path + dat_file)) :
            print('File ', data_path+dat_file, ' does not exist.')
            return None

        new_turn_dat = pd.read_csv(data_path + dat_file)

        # Clean the data file
        new_turn_dat.columns = [x.strip() for x in new_turn_dat.columns]
        new_turn_dat.rename(columns=lambda x: x.lower().replace(' ', '_'), inplace=True)

        # Remove the columns that we wont need
        new_turn_dat.drop(['c/a', 'unit', 'scp', 'linename'], axis=1, inplace=True)

        # Initialize the variables we're using for cleaning and summarizing
        delta_entry = [0]
        delta_exit = [0]
        new_turn_dat['date'] = pd.to_datetime(new_turn_dat['date'])
        new_turn_dat['time'] = pd.to_datetime(new_turn_dat['time'])
        new_turn_dat['time'] = new_turn_dat['time'].dt.time
        new_turn_dat['weekday'] = new_turn_dat['date'].dt.weekday_name

        # Loop through the rows of the dataframe
        for row in range(1, len(new_turn_dat)) :
            # Assume we're going to have a 0 change for each row
            entry_update = 0
            exit_update = 0

            # If we're still dealing with the same station
            if((new_turn_dat['station'][row] == new_turn_dat['station'][row - 1]) &
               (new_turn_dat['desc'][row] == 'REGULAR') &
               (new_turn_dat['date'][row] == new_turn_dat['date'][row - 1])) :

                # And we havn't changed a date
                if(new_turn_dat['entries'][row] > new_turn_dat['entries'][row - 1]) :
                    entry_update = (new_turn_dat['entries'][row] - 
                                new_turn_dat['entries'][row - 1])

                if(new_turn_dat['exits'][row] > new_turn_dat['exits'][row - 1]) :
                    exit_update = (new_turn_dat['exits'][row] - 
                                new_turn_dat['exits'][row - 1])

                # Check for 'outliers'
                if(entry_update > 20000) :
                    entry_update = 0

                if(exit_update > 20000) :
                    exit_update = 0

            # Update the lists
            delta_entry.append(entry_update)
            delta_exit.append(exit_update)

        # Add new columns to the data frame for the newly calculated data
        new_turn_dat['deltaEntry'] = delta_entry
        new_turn_dat['deltaExit'] = delta_exit

        if i == 0 :
            full_df = new_turn_dat
        else :
            full_df = pd.concat([full_df, new_turn_dat], axis=0, ignore_index = True)
    # end of for loop

    return full_df

def mta_data_pickle_dump(data, pickle_file) :
    '''
    mta_data_pickle_dump:
        data - data object to write to the pickle
        pickle_file - name of file to write to
    '''

    try :
        with open(pickle_file, 'wb') as to_write:
            pickle.dump(data, to_write)
    except OSError:
        print('Can\'t open ', pickle_file)

def mta_data_pickle_read(pickle_file) :
    '''
    mta_data_pickle_read:
        pickle_file - name of file with pickling data to read
    '''

    try :
        with open(pickle_file, 'rb') as read_file:
            data = pickle.load(read_file)
    except OSError:
        print('Can\'t open ', pickle_file)
        data = None

    return data
