# handle dataframes
import numpy as np
import pandas as pd
# retrieve data (custom module)
from get_data import get_dataset_db, get_naf_db

"""This file holds all functions used to transform the dataset retrieved from the SQL database to a 
DataFrame having format and information needed to feed the training ML SkLearn pipeline
"""

def suppr_nulls_nans(df):
    """This function drops rows and columns holding too much null data to be used for training purpose

    Args:
        df (pandas.DataFrame): df hoding the dataset

    Returns:
        df (pandas.DataFrame): df hoding the dataset after some nulls and nan values removal
    """
    # suppression des entreprises avec CA null
    df = df[df["Chiffre d'affaires net (Total) (FL) 2018 (€)"] > 0]
    # suppression des colonnes avec plus de 12% de NaN (sauf tranche d'effectif)
    cols_null_to_drop = ["Procédures collectives (type)", 
                         "Procédures collectives (date)", 
                         "Croissance CA", 
                         "Apport en comptes courants", 
                         "dont comptes courants d'associés de l'exercice N (EA2) 2017 (€)", 
                         "Chiffre d'affaires net (Total) (FL) 2017 (€)"]
    df = df.drop(cols_null_to_drop, axis=1)
    # passage des valeurs infinies en nan
    df = df.replace([np.inf, -np.inf], np.nan)
    return df


def merge_naf_v2(df, df_naf):
    """This function performs a left join between dataset and a table holding a sectorial mapping table
        in order to change sectorial granularity

    Args:
        df (pandas.DataFrame): dataframe holding the dataset
        df_naf (pandas.DataFrame): dataframe holding a mapping table between different sectorial granularity levels

    Returns:
        df (pandas.DataFrame): merged df
    """
    merged_df = pd.merge(df, df_naf, how = 'left', left_on = df['Code APE'], right_on = df_naf['code_ape'])
    merged_df = merged_df.drop(['key_0'], axis=1)
    assert merged_df.shape[0]==df.shape[0]
    assert merged_df.shape[1]==df.shape[1]+df_naf.shape[1]
    return merged_df


def remove_useless_cols_db(df):
    """This function drops some columns from DataFrame that are not relevant for a failure prediction purpose

    Args:
        df (pandas.DataFrame): dataframe holding the dataset

    Returns:
        df (pandas.DataFrame): dataframe holding the dataset
    """
    useless_cols = [
        "Dénomination",
        "Ville",
        "Emprunts remboursés en cours d’exercice (VK) 2018 (€)",
        "Tranche effectifs",
        "SIREN",
        "SIRET",
        "Code postal",
        "Emprunts souscrits en cours d’exercice - à plus d'un an et 5 ans au plus (VJ3) 2018 (€)",
        "Emprunts souscrits en cours d’exercice - à plus de 5 ans (VJ4) 2018 (€)",
        "Code APE",
        'index',
        'code_ape',
        'descriptif_code_ape',
        'a_615',
        'a_272',
        'a_129',
        'a_88',
        'a_64',
        'a_38',
        'a_10',
        "Emprunts souscrits en cours d’exercice - à 1 an au plus (VJ2) 2018 (€)"
    ]
    df = df.drop(useless_cols, axis=1)
    return df

def apply_categorical_dtypes(df):
    """This function converts 2 columns (Catégorie juridique (Niveau II) and a_21) of the input DataFrame into category dtypes

    Args:
        df (pandas.DataFrame) holding the dataset

    Returns:
        df (pandas.DataFrame) holding the dataset
    """
    df = df.astype({"Catégorie juridique (Niveau II)" : 'category', 
                            'a_21' : 'category'})
    return df

def remove_outliers(df, col):
    """This function removes outliers from a df's column.
    Outliers are values exceeding (above and underneath) 20 times the column's standard deviation 

    Args:
        df (pandas.DataFrame): df holding the dataset
        col (str): name of the column to be considered

    Returns:
        _type_: _description_
    """
    mean = np.mean(df[col], axis=0)
    sd = np.std(df[col], axis=0)
    minimum = mean - 20 * sd
    maximum = mean + 20 * sd
    df = df.loc[(df[col] > minimum) & (df[col] < maximum)]
    return df

#----MAIN----
def prepare_dataset_db():
    """This function is the main function of this module.
    It gets all information needed in the SQL database and then
    performs every transformation needed to the dataframe 
    in order to push the output DataFrame into a preprocessing and model-fitting SKlearn pipeline

    Returns:
        pandas.DataFrame: df holding all information in the appropriate format to be an input for the model pipeline
    """
    df = get_dataset_db() # 132 cols including target
    df = suppr_nulls_nans(df) # drops 6 cols => 126 cols
    df_naf = get_naf_db() 
    df = merge_naf_v2(df, df_naf) # adds 11 cols => 137 cols
    df = remove_useless_cols_db(df) # removes 21 cols => 116 cols (including target)
    df = apply_categorical_dtypes(df)
    for col in df.select_dtypes(np.number):
        df = remove_outliers(df, col)
    #assert df.shape[1]==116
    return df