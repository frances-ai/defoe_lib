from os import listdir
from os.path import isdir, join, dirname, abspath
from pathlib import Path


def create_nls_folder_path_mapping_file(result_filename, nls_data_folder_path):
    """
    This function records the filepaths of nls dataset in a text file. The required nls_data_folder_path is the path where
    your downloaded nls dataset is located. Inside the folder, it should have list of subfolders which stores xml files and images.
    Each subfolder has data for one document which can be a book, newspaper, et al. The result file will list the absolute paths
    of all subfolders and separate them with line break.
    :param result_filename: the filename of the result file, it will be saved in the same workspace where this python file is.
    :param nls_data_folder_path: the absolute path of the folder where the downloaded nls dataset is located.
    :return:
    """
    # Read nls data folder and subfolder's name
    dirs = [f for f in listdir(nls_data_folder_path) if isdir(join(nls_data_folder_path, f))]
    #print(dirs)

    # Write the absolute paths of these subfolders to result file.
    result_file = open(result_filename, 'w')
    for folder_name in dirs:
        result_file.write(join(nls_data_folder_path, folder_name) + '\n')
    pass


if __name__ == '__main__':
    current_path = Path(abspath(dirname(__file__))).absolute()
    data_mapping_file_path = str(current_path) + '/broadsides.txt'
    data_file_path = '/Users/lilinyu/Downloads/nls-data-broadsides'
    create_nls_folder_path_mapping_file(data_mapping_file_path, data_file_path)