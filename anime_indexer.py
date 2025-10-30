import sqlite3
from collections import Counter
import re  # For cleaning text
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from langdetect import detect

STOPWORDS = set(stopwords.words('english'))

def clean_title(title):
    if not title:
        return []
    title = title.lower()
    title = re.sub(r'[^a-z\s]', '', title)  # Remove non-letter characters (keep spaces)
    words = title.split()
    return [word for word in words if word not in STOPWORDS and len(word) > 1]

def word_indexer_from_title():
    # Connect to DB
    conn = sqlite3.connect('anime.db')
    cursor = conn.cursor()

    # Fetch both fields
    cursor.execute("SELECT title_english, title FROM anime")
    rows = cursor.fetchall()

    # Process and count
    word_counter = Counter()
    for title_english, title in rows:
        effective_title = title_english if title_english and title_english.strip() else title
        if effective_title:
            cleaned_words = clean_title(effective_title)
            word_counter.update(cleaned_words)

    conn.close()

    # Top 20
    top_words = word_counter.most_common(30)
    print("Most common words in anime titles (excluding stopwords):")
    for word, count in top_words:
        print(f"{word}: {count}")

if __name__ == "__main__":
    word_indexer_from_title()