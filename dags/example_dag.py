from __future__ import annotations

import pendulum
import requests

from airflow.decorators import dag, task

@dag(
    dag_id="simple_requests_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "requests", "http"],
    schedule=None,  # This DAG will run manually or be triggered
    doc_md="""
    ### Simple Requests DAG

    This DAG demonstrates how to use the `requests` package within an Airflow task
    to make an HTTP GET request to a public API (JSONPlaceholder).
    It fetches a list of users and then a specific user's posts.
    """,
)
def simple_requests_workflow():
    @task
    def fetch_users_data():
        """
        Fetches a list of users from JSONPlaceholder and returns their IDs.
        """
        api_url = "https://jsonplaceholder.typicode.com/users"
        print(f"Making GET request to: {api_url}")
        try:
            response = requests.get(api_url, timeout=10)  # Added timeout for robustness
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
            users = response.json()
            user_ids = [user["id"] for user in users]
            print(f"Successfully fetched {len(users)} users. User IDs: {user_ids}")
            return user_ids
        except requests.exceptions.RequestException as e:
            print(f"Error fetching users data: {e}")
            raise  # Re-raise the exception to mark the task as failed

    @task
    def fetch_user_posts(user_id: int):
        """
        Fetches posts for a given user ID from JSONPlaceholder.
        """
        api_url = f"https://jsonplaceholder.typicode.com/users/{user_id}/posts"
        print(f"Making GET request to: {api_url} for user ID: {user_id}")
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            posts = response.json()
            print(f"Successfully fetched {len(posts)} posts for user ID {user_id}.")
            # In a real scenario, you might store this data, process it, etc.
            # For demonstration, we just print a snippet.
            if posts:
                print(f"Example post title for user {user_id}: '{posts[0]['title']}'")
            return posts
        except requests.exceptions.RequestException as e:
            print(f"Error fetching posts for user {user_id}: {e}")
            raise

    @task
    def process_all_data(all_users_data: list[int], all_posts_data: list[list[dict]]):
        """
        A final task to indicate data processing (placeholder).
        """
        print(f"Received data for {len(all_users_data)} users.")
        total_posts_fetched = sum(len(posts) for posts in all_posts_data)
        print(f"Total posts fetched across all users: {total_posts_fetched}")
        print("Data processing complete.")

    # Task definitions
    user_ids = fetch_users_data()
    user_posts = fetch_user_posts.expand(user_id=user_ids) # Dynamically creates tasks for each user_id

    # Define task dependencies
    # The 'user_posts' tasks will run after 'fetch_users_data' completes
    # The 'process_all_data' task will run after all 'user_posts' tasks complete
    process_all_data(user_ids, user_posts)

# Instantiate the DAG
#simple_requests_dag()
simple_requests_workflow()
