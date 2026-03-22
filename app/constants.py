"""Static UI / form choice lists."""

_TIMEZONE_CHOICES = [
    ("UTC", "UTC"),
    ("America/New_York", "America/New_York"),
    ("America/Chicago", "America/Chicago"),
    ("America/Denver", "America/Denver"),
    ("America/Los_Angeles", "America/Los_Angeles"),
    ("America/Phoenix", "America/Phoenix"),
    ("America/Anchorage", "America/Anchorage"),
    ("America/Toronto", "America/Toronto"),
    ("America/Vancouver", "America/Vancouver"),
    ("Europe/London", "Europe/London"),
    ("Europe/Paris", "Europe/Paris"),
    ("Europe/Berlin", "Europe/Berlin"),
    ("Europe/Amsterdam", "Europe/Amsterdam"),
    ("Europe/Rome", "Europe/Rome"),
    ("Australia/Sydney", "AEDT/AEST (Australia/Sydney)"),
    ("Australia/Brisbane", "AEST (Australia/Brisbane)"),
    ("Australia/Melbourne", "Australia/Melbourne"),
    ("Australia/Perth", "Australia/Perth"),
    ("Australia/Adelaide", "Australia/Adelaide"),
    ("Asia/Tokyo", "Asia/Tokyo"),
    ("Asia/Shanghai", "Asia/Shanghai"),
    ("Asia/Singapore", "Asia/Singapore"),
    ("Pacific/Auckland", "Pacific/Auckland"),
]

_MOVIE_GENRE_OPTIONS = [
    "Action",
    "Adventure",
    "Animation",
    "Comedy",
    "Crime",
    "Documentary",
    "Drama",
    "Family",
    "Fantasy",
    "History",
    "Horror",
    "Music",
    "Mystery",
    "Romance",
    "Science Fiction",
    "Thriller",
    "TV Movie",
    "War",
    "Western",
]

# Values must match Emby People[].Type (storage is canonical casing).
_PEOPLE_CREDIT_OPTIONS: list[tuple[str, str]] = [
    ("Actor", "Cast (actors)"),
    ("Director", "Directors"),
    ("Writer", "Writers"),
    ("Producer", "Producers"),
    ("GuestStar", "Guest stars"),
]
