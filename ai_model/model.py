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
# scoring
from sklearn.model_selection import cross_val_score
# export model
import joblib
# save model artifacts
import mlflow
import mlflow.sklearn

"""This file is the main file of the ai_model folder. 
Running it will perform all operations needed to :
- retrieve data from SQL database (using get_data through prepare_data)
- transform data in a proper way to feed the ML pipeline (using prepare_data)
- perform preprocessing and training a model on the input data (functions held in this file)
- store model information (params/metrics) using MLflow (functions held in this file)
- saving the fit model as 'fail_pred_model.joblib' (functions held in this file)
"""

def prepro(df):
    """This function creates a preprocessing pipeline for modeling purpose.
    The preprocessing pipeline includes a PCA(n_components=64) over numerical features

    Args:
        df (pandas.DataFrame): df holding dataset used to train the model

    Returns:
        prepro (sklearn.pipeline.Pipeline): preprocessing pipeline
    """
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
    """This function fits a Logistic Regression model with following hyperparameters:
        - solver='liblinear'
        - penalty='l1'
        - C=0.005
        - class_weight='balanced'
        Once the model is fitted, its information (params/metrics) are stored using MLflow 
        - to local server on 0.0.0.0:5000
        - to local repository with default settings (mlrun/)
    Args:
        df (pandas.DataFrame): df holding the dataset used to train the model
        prepro (sklearn.pipeline.Pipeline): preprocessing pipeline

    Returns:
        model (sklearn.pipeline.Pipeline): sklearn fit model including 
        preprocessing pipeline and Logistic regression
    """
    
    # "mlruns/" stands for the default storage method
    # below line to be run in terminal to lauch gunicorn server :
    # mlflow server --backend-store-uri mlruns/ --default-artifact-root mlruns/ --host 0.0.0.0 --port 5000

    # declare mlflow settings
    local_server_uri = "http://0.0.0.0:5000" # set to local server URI
    mlflow.set_tracking_uri(local_server_uri)
    mlflow.get_tracking_uri()
    exp_name = 'Logistic_PCA'
    mlflow.set_experiment(exp_name)

    with mlflow.start_run():
        # execute model
        model = make_pipeline(prepro, 
                            LogisticRegression(solver='liblinear', penalty='l1', C=0.005, class_weight='balanced'))
        X=df.drop(["default"], axis=1)
        y=df["default"]
        model.fit(X, y)
        # evaluate metrics
        accuracy = cross_val_score(model, X, y).mean()
        # log parameters, metrics and model to MLflow
        mlflow.log_params(model.named_steps['logisticregression'].get_params())
        mlflow.log_metrics({"accuracy": accuracy})
        mlflow.sklearn.log_model(model, "final_model")

        return model

def save_model(model, model_name):
    """This function saves the fit model to a joblib file

    Args:
        model (sklearn.pipeline.Pipeline): sklearn fit model including
            preprocessing pipeline and Logistic regression
        model_name (str): model name (needs to have a .joblib extension)
    """
    joblib.dump(model, model_name)


def main():
    """This function is the main function. Running it will perform all operations needed to :
            - retrieve data from SQL database (using get_data through prepare_data)
            - transform data in a proper way to feed the ML pipeline (using prepare_data)
            - perform preprocessing and training a model on the input data (functions held in this file)
            - saving the fit model as 'fail_pred_model.joblib' (functions held in this file)
    """
    df = prepare_dataset_db()
    prepro_pca = prepro(df)
    model = fit_model(df, prepro_pca)
    save_model(model, "fail_pred_model.joblib")

if __name__ == "__main__":
    main()



