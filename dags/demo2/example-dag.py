from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import logging
import os
import MySQLdb
import pyodbc

# TensorFlow imports
import tensorflow as tf

# LightGBM imports
import lightgbm as lgb
import pandas as pd
from sklearn.metrics import mean_squared_error

# XGBoost imports
import xgboost as xgb
from sklearn import datasets
from sklearn.cross_validation import train_test_split
from sklearn.datasets import dump_svmlight_file

log = logging.getLogger("example_dag_02")

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime(2018, 1, 1),
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 0
}

dag = DAG('example_dag_02',
			max_active_runs=1,
			schedule_interval=timedelta(minutes=5),
			default_args=default_args,
			catchup=False)


def test_mysql(*args, **kwargs):
	#db = MySQLdb.connect(host="mysql",user="root",passwd="root",db="airflow" )
	#rsor = db.cursor()
	#cursor.execute("SELECT VERSION()")
	log.info("Do not have a MySQL database to test against...")

t_test_mysql = PythonOperator(
    dag=dag,
    task_id='t_test_mysql',
    python_callable=test_mysql,
    trigger_rule="all_done",
    provide_context=True
)

def test_pyodbc(*args, **kwargs):
	# conn_str = (
 #    #"DRIVER={PostgreSQL Unicode};"
 #    #"DATABASE=postgres;"
 #    #"UID=postgres;"
 #    #"PWD=postgres;"
 #    #"SERVER=postgres;"
 #    #"PORT=5432;"
 #    "DSN=my-connector"
 #    )
	# cnxn = pyodbc.connect(conn_str)
	# crsr = conn.execute("SELECT * FROM pg_catalog.pg_tables")
	# row = crsr.fetchone()
	# log.info(row)
	# crsr.close()
	# conn.close()
	log.info("Missing postgres driver - mess with this later...")

t_test_pyodbc = PythonOperator(
    dag=dag,
    task_id='t_test_pyodbc',
    python_callable=test_pyodbc,
		trigger_rule="all_done",
    provide_context=True
)

def test_xgboost(*args, **kwargs):
	iris = datasets.load_iris()
	X = iris.data
	y = iris.target
	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
	dtrain = xgb.DMatrix(X_train, label=y_train)
	dtest = xgb.DMatrix(X_test, label=y_test)
	dump_svmlight_file(X_train, y_train, 'dtrain.svm', zero_based=True)
	dump_svmlight_file(X_test, y_test, 'dtest.svm', zero_based=True)
	dtrain_svm = xgb.DMatrix('dtrain.svm')
	dtest_svm = xgb.DMatrix('dtest.svm')
	param = {
	    'max_depth': 3,  # the maximum depth of each tree
	    'eta': 0.3,  # the training step for each iteration
	    'silent': 1,  # logging mode - quiet
	    'objective': 'multi:softprob',  # error evaluation for multiclass training
	    'num_class': 3}  # the number of classes that exist in this datset
	num_round = 20  # the number of training iterations
	bst = xgb.train(param, dtrain, num_round)
	bst.dump_model('dump.raw.txt')

t_test_xgboost = PythonOperator(
    dag=dag,
    task_id='t_test_xgboost',
    python_callable=test_xgboost,
		trigger_rule="all_done",
    provide_context=True
)

def test_lightgbm(*args, **kwargs):
	log.info('Loading data...')
	# load or create your dataset
	df_train = pd.read_csv(os.environ['AIRFLOW_HOME'] + '/dags/demo2/regression.train', header=None, sep='\t')
	df_test = pd.read_csv(os.environ['AIRFLOW_HOME'] + '/dags/demo2/regression.test', header=None, sep='\t')

	y_train = df_train[0]
	y_test = df_test[0]
	X_train = df_train.drop(0, axis=1)
	X_test = df_test.drop(0, axis=1)

	# create dataset for lightgbm
	lgb_train = lgb.Dataset(X_train, y_train)
	lgb_eval = lgb.Dataset(X_test, y_test, reference=lgb_train)

	# specify your configurations as a dict
	params = {
	    'boosting_type': 'gbdt',
	    'objective': 'regression',
	    'metric': {'l2', 'l1'},
	    'num_leaves': 31,
	    'learning_rate': 0.05,
	    'feature_fraction': 0.9,
	    'bagging_fraction': 0.8,
	    'bagging_freq': 5,
	    'verbose': 0
	}

	log.info('Starting training...')
	# train
	gbm = lgb.train(params,
	                lgb_train,
	                num_boost_round=20,
	                valid_sets=lgb_eval,
	                early_stopping_rounds=5)

	log.info('Saving model...')
	# save model to file
	gbm.save_model('model.txt')

	log.info('Starting predicting...')
	# predict
	y_pred = gbm.predict(X_test, num_iteration=gbm.best_iteration)
	# eval
	log.info('The rmse of prediction is:', mean_squared_error(y_test, y_pred) ** 0.5)

t_test_lightgbm = PythonOperator(
    dag=dag,
    task_id='t_test_lightgbm',
    python_callable=test_lightgbm,
		trigger_rule="all_done",
    provide_context=True
)

def test_tensorflow(*args, **kwargs):
	const = tf.constant(2.0, name="const")
    
	# create TensorFlow variables
	b = tf.Variable(2.0, name='b')
	c = tf.Variable(1.0, name='c')

	# now create some operations
	d = tf.add(b, c, name='d')
	e = tf.add(c, const, name='e')
	a = tf.multiply(d, e, name='a')
	
	# setup the variable initialisation
	init_op = tf.global_variables_initializer()

	# start the session
	with tf.Session() as sess:
	    # initialise the variables
	    sess.run(init_op)
	    # compute the output of the graph
	    a_out = sess.run(a)
	    log.info("Variable a is {}".format(a_out))

t_test_tensorflow = PythonOperator(
    dag=dag,
    task_id='t_test_tensorflow',
    python_callable=test_tensorflow,
		trigger_rule="all_done",
    provide_context=True
)

t_test_pyodbc.set_upstream(t_test_mysql)
t_test_mysql.set_upstream(t_test_xgboost)
t_test_xgboost.set_upstream(t_test_lightgbm)
t_test_lightgbm.set_upstream(t_test_tensorflow)

