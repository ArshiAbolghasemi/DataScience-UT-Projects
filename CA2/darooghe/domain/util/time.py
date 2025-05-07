from enum import Enum


class TimeOfDay(Enum):
    MORNING = (6, 11, "morning")
    AFTERNOON = (12, 16, "afternoon")
    EVENING = (17, 20, "evening")
    NIGHT = (21, 5, "night")

    @property
    def start(self):
        return self.value[0]

    @property
    def end(self):
        return self.value[1]

    @property
    def label(self):
        return self.value[2]


JAVA_DATE_FORMAT_YEAR_MONTH = "yyyy-MM"
