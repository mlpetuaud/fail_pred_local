# handle dataframes
import numpy as np
import pandas as pd
# get and prepare data (custom modules)
from prepare_data import prepare_dataset_db
# pipelines
from sklearn.pipeline import make_pipeline
from sklearn.compose import ColumnTransformer
# preprocessing
from sklearn.decomposition import PCA
from sklearn.preprocessing import RobustScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
# models
from sklearn.linear_model import LogisticRegression
# export model
import joblib


def prepro(df):
#dispatch cols according to their data types
    num_cols = df.select_dtypes(np.number).columns.drop("default")
    categ_cols = ["Catégorie juridique (Niveau II)", 'a_21']
    categorical_pipeline = make_pipeline(OneHotEncoder(handle_unknown='ignore'))
    prepro_pca = ColumnTransformer([("pipe_cat", 
                                        categorical_pipeline, 
                                        categ_cols),
                                    ('just_pca', 
                                        make_pipeline(
                                        SimpleImputer(strategy='constant', fill_value=0), 
                                        RobustScaler(),
                                        PCA(n_components=64)),
                                        num_cols)],
                                    remainder="drop")
    return prepro_pca

def fit_model(df, prepro):
    final_model = make_pipeline(prepro, 
                        LogisticRegression(solver='liblinear', penalty='l1', C=0.005, class_weight='balanced'))
    X=df.drop(["default"], axis=1)
    y=df["default"]
    final_model.fit(X, y)
    return final_model

def save_model(model, model_name):
    joblib.dump(model, model_name)

def main():
    df = prepare_dataset_db()
    prepro_pca = prepro(df)
    model = fit_model(df, prepro_pca)
    save_model(model, "fail_pred_model.joblib")

if __name__ == "__main__":
    main()


