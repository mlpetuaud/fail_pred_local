# handle dataframes
import numpy as np
import pandas as pd
# retrieve data (custom module)
from get_data import get_dataset_db, get_naf_db


def suppr_nulls_nans(df):
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
    merged_df = pd.merge(df, df_naf, how = 'left', left_on = df['Code APE'], right_on = df_naf['code_ape'])
    merged_df = merged_df.drop(['key_0'], axis=1)
    assert merged_df.shape[0]==df.shape[0]
    assert merged_df.shape[1]==df.shape[1]+df_naf.shape[1]
    return merged_df


def remove_useless_cols_db(df):
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
    df = df.astype({"Catégorie juridique (Niveau II)" : 'category', 
                            'a_21' : 'category'})
    return df

def remove_outliers(df, col):
    mean = np.mean(df[col], axis=0)
    sd = np.std(df[col], axis=0)
    minimum = mean - 20 * sd
    maximum = mean + 20 * sd
    df = df.loc[(df[col] > minimum) & (df[col] < maximum)]
    return df

def prepare_dataset_db():
    df = get_dataset_db() # 132 cols including target
    df = suppr_nulls_nans(df) # drops 6 cols => 126 cols
    df_naf = get_naf_db() 
    df = merge_naf_v2(df, df_naf) # adds 11 cols => 137 cols
    df = remove_useless_cols_db(df) # removes 21 cols => 116 cols (including target)
    df = apply_categorical_dtypes(df)
    for col in df.select_dtypes(np.number):
        df = remove_outliers(df, col)
    assert df.shape[1]==116
    return df