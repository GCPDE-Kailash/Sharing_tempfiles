from google.cloud import pubsub_v1
import csv
import time
import os
#from df_processer import process1

project = 'df-pipeline-project-19062024'
publisher_path = 'projects/df-pipeline-project-19062024/topics/movies'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'df-pipeline-project-sa-authentication_key.json'

input_file = 'Input\ml-latest\movies.csv'
publisher = pubsub_v1.PublisherClient()

def process():
    print(f"Publishing started for messages on {publisher_path}")
    #while True:   #uncommented ot when data will come streaming
    try:
        publish_msg()
    except Exception as error:
        print(f"df pipeline flow process Job FAILED; {error}")
def publish_msg():
    with open('Input\ml-latest\movies.csv', 'rb') as file:
        count = 1
        for row in file:
            print('Publishing Topic message :',count)
            count += 1
            response = publisher.publish(topic=publisher_path, data=row)
            # print('Message {} published.'.format(response.result()))
            # df_processer.process1(response.result())
            time.sleep(1)

if __name__ == '__main__':
    process()
          


