# Goodreads-Rebot : A Reddit Book Bot, Ready for Industrialization on Google Cloud Platform

This bot fetches posts from a specified subreddit, searches for book titles mentioned by users in {{double braces}}, queries a database to find info on those books, and replies to the posts with additional details.

## How it Works

The bot has a 3-step workflow:

1. **Crawling** - Fetches recent posts from the subreddit and saves new post IDs to a BigQuery table.

2. **Matching** - Checks the BigQuery table for new post IDs, extracts book titles from the post text, queries the database to find book info, and prepares a reply. 

3. **Replying** - Posts the prepared reply as a comment on the original post.

The main classes handling each step are:

- `Reader` - Crawls subreddit looking for summoning pattern `{{something}}` and saves the post IDs to reply to.
- `Matcher` - Matches book titles, fetches additional information, formats reply, posts on Reddit.
- `Bot` - Initializes Reader and Matcher objects and runs workflow

## Configuration

The bot behavior is configured via the `config.json` file:

- `subreddit` - Subreddit name to crawl 
- `limit` - Max number of posts to fetch per crawl
- `min_ratio` - Minimum matching score (/100) to accept a book title match
- `table_*` - Names of the BigQuery tables 

## Overview of Database Schema

The bot mainly uses the following BigQuery tables:

**`table_dim_books`** - Main book info table

- `master_grlink` - Goodreads link for the book
- `short_title` - Title without Series names (if so)
- `first_author` - Main author
- `series_title` - Series name if part of a series, NULL if not
- `book_number` - Book number within series, NULL if not
- `tags` - Array of topic tags  
- `summary` - Book summary text
- ...plus other metadata fields

**`table_crawl_dates`** - Tracks last crawl timestamp per subreddit 

- `subreddit` - Subreddit name
- `crawl_timestamp` - Last crawl time for subreddit 

**`table_to_match`** - New post IDs to process 

- `subreddit` - Subreddit of post
- `post_id` - Reddit post ID  
- `post_timestamp` - Post creation time
- `post_type` - `submission` or `comment`

**`table_reply_logs`** - Logs replies posted by bot

- `post_id` - Reddit post ID
- `reply_id` - Reddit reply ID
- `score` - Book title match score
- `master_grlink` - Link to book page

## Running the Bot

To start the bot:

1. Set up BigQuery credentials 
2. Configure `config.json`
3. Run the command: `python main.py --config config.json`

The `main.py` script will initialize the `Reader` and `Matcher` objects and run through the workflow ONCE: one crawling, one matching. Adapt the file for more advanced behavior.
