from airflow.decorators import dag, task
import pendulum
import os
import requests
import xmltodict
from dotenv import load_dotenv
from airflow.operators.bash import BashOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from gradio_client import Client

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"
EPISODE_FOLDER = "downloads"
load_dotenv()


@dag(dag_id="podcast_summary2", start_date=pendulum.datetime(2023, 8, 31), schedule_interval="@daily", catchup=False)
def podcast_summary2():

    create_database = BashOperator(
        task_id="create_database",
        bash_command="""
            rm -f episodes.db
            sqlite3 episodes.db "VACUUM;"
        """,
    )
    create_table = SqliteOperator(
        task_id="create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS episodes(
                link TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                filename TEXT NOT NULL,
                published_date TEXT NOT NULL,
                description TEXT NOT NULL,
                transcript TEXT,
                ai_text TEXT
            );
        """,
        sqlite_conn_id="podcasts",
    )

    @task() # one task in the dag
    def get_episodes():
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"][:5] # list of episodes, we usually dont know this upfront
        print("Found {} episodes".format(len(episodes)))
        return episodes

    podcast_episodes = get_episodes()
    create_table.set_downstream(podcast_episodes)
    create_database.set_downstream(create_table)

    @task()
    def insert_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        stored = hook.get_pandas_df("SELECT * FROM episodes")
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored["link"].values:
                filename = episode["link"].split("/")[-1] + ".mp3"
                new_episodes.append(
                    [episode["link"], episode["title"], filename, episode["pubDate"], episode["description"]]
                )
        hook.insert_rows("episodes", new_episodes, target_fields=["link", "title", "filename", "published_date", "description"])

    new_episodes = insert_episodes(podcast_episodes)

    @task()
    def download_episodes(episodes):
        for episode in episodes[:5]:
            filename = episode["link"].split("/")[-1] + ".mp3"
            path = "downloads/" + filename
            if os.path.exists(path):
                print("File already downloaded")
                continue
            else:
                print("Downloading {}".format(filename))
                r = requests.get(episode["enclosure"]["@url"])
                with open(path, "wb+") as f:
                    f.write(r.content)

    audio_files = download_episodes(podcast_episodes)

    @task()
    def speech_to_text(audio_files, new_episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        untranscribed_episodes = hook.get_pandas_df("SELECT * from episodes")

        for _, row in untranscribed_episodes.iterrows():
            if (not row["transcript"]) or len(row["transcript"]) < 1000:
                print(f"Transcribing {row['filename']}")
                filepath = os.path.join(EPISODE_FOLDER, row["filename"])
                client = Client("https://sanchit-gandhi-whisper-jax.hf.space/")
                transcript = client.predict(
                    filepath,	# str (filepath or URL to file) in 'Audio file' Audio component
                    "transcribe",	# str in 'Task' Radio component
                    False,	# bool in 'Return timestamps' Checkbox component
                    api_name="/predict_1"
                )
                transcript = transcript[0].replace('"', "'")
                #transcript = "testing testing 123"
                #print(transcript)
                # Define your SQL statement to edit rows
                sql_statement = f'UPDATE episodes SET transcript="{transcript}" WHERE filename="{row["filename"]}"';
                print(sql_statement)
                # Execute the SQL statement
                hook.run(sql_statement)

    transcribed_episodes = speech_to_text(audio_files, new_episodes)

    @task()
    def summarize_episodes(transcribed_episodes):
        from revChatGPT.V1 import Chatbot
        chatbot = Chatbot(config={
            "api_key": os.getenv("OPEN_AI_TOKEN"),
        })
        hook = SqliteHook(sqlite_conn_id="podcasts")
        transcribed_episodes = hook.get_pandas_df("SELECT * from episodes")

        for _, row in transcribed_episodes.iterrows():
            if (not row["ai_text"]) or len(row["ai_text"]) < 100:
                print(f"Summarizing {row['filename']}")
                prompt = f'summarize this text """{row["transcript"]}"""'
                response = ""
                for data in chatbot.ask(
                prompt
                ):
                    response = data["message"]
                text = response.replace('"', "'")
                sql_statement = f'UPDATE episodes SET ai_text="AI SUMMARY: {text}" WHERE filename="{row["filename"]}"';
                print(sql_statement)
                # Execute the SQL statement
                hook.run(sql_statement)

    summarize_episodes(transcribed_episodes)

summary = podcast_summary2()