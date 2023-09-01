# Data pipeline for summarizing Marketplace podcast episodes.
1. Have Airflow installed and running, along with SQLite.
2. Create a virtual environment and install the requirements.
3. Create a .env file, and paste the `accessToken` from [here](https://chat.openai.com/api/auth/session) as the value for `OPEN_AI_TOKEN`.
4. Run DAG.