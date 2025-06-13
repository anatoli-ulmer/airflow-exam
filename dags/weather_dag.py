from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump
from datetime import datetime
import os
import json
import requests
from time import sleep
import pandas as pd


API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')
RAW_DIR = "/app/raw_files"
PROCESSED_DIR = "/app/clean_data"


def get_open_weather_data():
        
    # Get variable or use default list
    cities_str = Variable.get("cities", default_var=None)
    if cities_str is None:
        cities = ["Berlin", "Paris", "London"]
    else:
        cities = json.loads(cities_str)
        
    # Collect data
    responses = []
    dt = datetime.now()
    print(f"Requesting weather data for {cities} ({dt.strftime('%Y-%m-%d %H:%M:%SZ')})")
    for city in cities:
        r = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}")
        data_city = r.json()
        print(f"Received data: {data_city['main']['temp']-273.15:.1f}°C in {data_city['name']}")
        responses.append(data_city)
        sleep(1)
    
    # Save results
    filename = dt.strftime("%Y-%m-%d %H:%M.json")
    os.makedirs(RAW_DIR, exist_ok=True)
    savepath = os.path.join(RAW_DIR, filename)
    print(f"Saving results to {savepath}")
    with open(savepath, 'w') as f:
        json.dump(responses, f)


def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = RAW_DIR
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    df.to_csv(os.path.join(PROCESSED_DIR, filename), index=False)
    
    

def compute_model_score(model, **kwargs):
    X = pd.read_csv(os.path.join(PROCESSED_DIR, 'X.csv'))
    y = pd.read_csv(os.path.join(PROCESSED_DIR, 'y.csv'))
    
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    model_name = model.__class__.__name__
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key=f'{model_name}_score', value=model_score)
    # return model_score


def train_and_save_model(**kwargs):
    task_instance = kwargs['task_instance']
    lr_score = task_instance.xcom_pull(key='LinearRegression_score', task_ids='train_linear_regression')
    dt_score = task_instance.xcom_pull(key='DecisionTreeRegressor_score', task_ids='train_decision_tree_regresor')
    rf_score = task_instance.xcom_pull(key='RandomForestRegressor_score', task_ids='train_random_forest_regressor')
    
    scores = {'lr_score': lr_score, 'dt_score': dt_score, 'rf_score': rf_score}
    actions = {'lr_score': LinearRegression(), 
               'dt_score': DecisionTreeRegressor(), 
               'rf_score': RandomForestRegressor()}
    min_score_name = max(scores, key=scores.get)
    print(f"Model scores: lr_score={lr_score}, dt_score={dt_score}, rf_score={rf_score}")
    print(f"{min_score_name} has largest neg_mean_square_error, choosing model for training accordingly")
    model = actions[min_score_name]

    # load dada   
    X = pd.read_csv(os.path.join(PROCESSED_DIR, 'X.csv'))
    y = pd.read_csv(os.path.join(PROCESSED_DIR, 'y.csv'))
    
    # training the model
    model.fit(X, y)
    
    # saving model
    path_to_model=os.path.join(PROCESSED_DIR, 'model.pckl')
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)


def prepare_data(path_to_data=os.path.join(PROCESSED_DIR, 'fulldata.csv')):
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    features.to_csv(os.path.join(PROCESSED_DIR, 'X.csv'))
    target.to_csv(os.path.join(PROCESSED_DIR, 'y.csv'))
    # return features, target

    
with DAG(
    dag_id='weather_dag',
    tags=['exam', 'weather', 'datascientest'],
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 6, 10, 0, 0, 0),
    },
    schedule_interval='* * * * *',
    catchup=False
) as dag:
    
    get_raw_files = PythonOperator(
        task_id='get_raw_files',
        python_callable=get_open_weather_data,
    )
    
    transform_tail_into_csv = PythonOperator(
        task_id='transform_tail_into_csv',
        python_callable=transform_data_into_csv,
        op_kwargs={'n_files': 20, 'filename': 'data.csv'}
    )
    
    transform_all_into_csv = PythonOperator(
        task_id='transform_all_into_csv',
        python_callable=transform_data_into_csv,
        op_kwargs={'filename': 'fulldata.csv'}
    )
    
    prepare_data_for_training = PythonOperator(
        task_id='prepare_data_for_training',
        python_callable=prepare_data,
        op_kwargs={'path_to_data': os.path.join(PROCESSED_DIR, 'fulldata.csv')}
    )
    
    train_linear_regression = PythonOperator(
        task_id='train_linear_regression',
        python_callable=compute_model_score,
        op_args=[LinearRegression()]
    )
    
    train_decision_tree_regresor = PythonOperator(
        task_id='train_decision_tree_regresor',
        python_callable=compute_model_score,
        op_args=[DecisionTreeRegressor()]
    )
    
    train_random_forest_regressor = PythonOperator(
        task_id='train_random_forest_regressor',
        python_callable=compute_model_score,
        op_args=[RandomForestRegressor()]
    )
    
    evaluate_and_save_model = PythonOperator(
        task_id='evaluate_and_save_model',
        python_callable=train_and_save_model
    )
    
    get_raw_files >> [transform_tail_into_csv, transform_all_into_csv]
    transform_all_into_csv >> prepare_data_for_training
    prepare_data_for_training >> [train_linear_regression, train_decision_tree_regresor, train_random_forest_regressor]
    [train_linear_regression, train_decision_tree_regresor, train_random_forest_regressor] >> evaluate_and_save_model