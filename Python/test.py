from sql_connector import get_sql_connection
import pandas as pd

# Establish database connection
conn = get_sql_connection()
cursor = conn.cursor()

# Execute query and load results into DataFrame
query = """
SELECT vh.ContentID, vh.RegionID, COUNT(vh.UserID) AS WatchCount,
       SUM(vh.Watchtime) AS TotalWatchTime, c.Duration
FROM ViewingHistory vh
JOIN Contents c ON vh.ContentID = c.ContentID
GROUP BY vh.ContentID, vh.RegionID, c.Duration
"""
df = pd.read_sql(query, conn)

# Calculate popularity score
df['PopularityScore'] = (df['WatchCount'] / df['WatchCount'].max()) * 0.5 + (df['TotalWatchTime'] / df['Duration']) * 0.5

# Update database with calculated scores
for _, row in df.iterrows():
    cursor.execute("""
    MERGE INTO ContentPopularity AS target
    USING (VALUES (?, ?, ?)) AS source (ContentID, RegionID, PopularityScore)
    ON target.ContentID = source.ContentID AND target.RegionID = source.RegionID
    WHEN MATCHED THEN
        UPDATE SET PopularityScore = source.PopularityScore
    WHEN NOT MATCHED THEN
        INSERT (ContentID, RegionID, PopularityScore)
        VALUES (source.ContentID, source.RegionID, source.PopularityScore);
    """, row['ContentID'], row['RegionID'], row['PopularityScore'])

# Commit changes and clean up
conn.commit()
cursor.close()
conn.close()
print("Popularity scores updated successfully")