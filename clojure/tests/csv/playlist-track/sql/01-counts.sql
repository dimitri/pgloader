-- Good rows must be loaded; the one bad FK row must be rejected.
-- 8715 real PlaylistTrack rows + 1 injected bad row = 8716 total in the CSV.
SELECT COUNT(*) AS loaded FROM csv.playlist_track;
