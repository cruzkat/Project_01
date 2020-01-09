import sys
import mta_data_clean as mc

def main() :
    pickle_file = 'mta_data_pickle'
    data_path = '../data/'
    '''
    data_file_list = ['turnstile_191102.txt',
                 'turnstile_191109.txt',
                 'turnstile_191116.txt',
                 'turnstile_191130.txt',
                 'turnstile_191207.txt',
                 'turnstile_191214.txt',
                 'turnstile_191221.txt',
                 'turnstile_191228.txt'
                 'turnstile_200104.txt']
    '''
    data_file_list = ['turnstile_191102.txt', 'turnstile_191207.txt' ]

    df = mc.mta_data_clean(data_path, data_file_list)
    print(df.head())
    
    mc.mta_data_pickle_dump(df, pickle_file)

# Standard boilerplate to call the main() function.
if __name__ == '__main__':
  main()
