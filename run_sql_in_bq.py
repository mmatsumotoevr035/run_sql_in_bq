# このスクリプトは以下のライブラリに依存しています
# 依存ライブラリのインストールにはpipを使用してください

# pip install --upgrade google-cloud-bigquery[bqstorage,pandas]
# pip install --upgrade pip
# pip install google-cloud
# pip install google-cloud-storage
# pip install pydata-google-auth
# pip install pandas-gbq


import subprocess
import pydata_google_auth
import os
import pandas as pd
from google.cloud import bigquery



# 認証を通す
credentials = pydata_google_auth.get_user_credentials(
['https://www.googleapis.com/auth/bigquery'],
)



# Google drive共有フォルダマウント
# ※ブラウザでGoogleログイン認証が求められるので対応する
scopes = "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/cloud-platform"
command = ["gcloud", "auth", "application-default", "login", "--scopes=" + scopes]

try:
    subprocess.run(command, check=True, shell=True)
    print("Command executed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error executing command: {e}")



# 引数file_pathに記載したパスのSQLファイルを読み込む
def read_sql_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as sql_file:
        query = sql_file.read()
    return query



# read_sql_file()で読み込んだSQLをBQで実行⇒結果をcsvに保存
def execute_sql_queries_in_folder(folder_path):
    # フォルダ内のすべてのSQLファイルをリストに登録
    sql_files = [f for f in os.listdir(folder_path) if f.endswith('.sql')]

    # 念のためプロジェクトID指定
    project_id = 'tableau-d2cr'

    # すべてのSQLファイルにつきtry:以下のコードを実行
    for sql_file in sql_files:
        sql_file_path = os.path.join(folder_path, sql_file)
        query = read_sql_file(sql_file_path)

        # パス中のファイル名だけ取り出す　参考；https://aiacademy.jp/media/?p=1584
        sql_file_name = os.path.splitext(os.path.basename(sql_file_path))[0]

        # csv名をファイル名から定義
        new_csv_file_path = r'保存したいcsvのフォルダパス' + f'\{sql_file_name}.csv'
        # f文字列等について参考：https://note.nkmk.me/python-f-strings/

        try:
            # クエリ実行⇒データフレーム格納
            query_job = pd.read_gbq(query, project_id, dialect='standard', credentials=credentials)
            print(f"Query executed successfully.")

            # データフレームをcsv化 ※複数カラムある時はヘッダー有りに切り替える(header=Falseを削除)
            query_job.to_csv(new_csv_file_path, index=False, header=False)

            print(f"Result saved to {new_csv_file_path}")
            
        except Exception as e:
            print(f"Error executing query: {str(e)}")


folder_path = r'SQLファイルを格納したフォルダパス'
##フォルダパスをraw文字列（r接頭辞を付ける）として指定

execute_sql_queries_in_folder(folder_path)


# （メモ）データフレームをcsv化せず、ただ表示したいだけなら、csv関連のコードを削除またはコメントアウトし、「print(データフレーム名)」でOK