from pydantic import BaseModel, ValidationError, validator
from typing import List
import json

# Define Pydantic models for data validation
class Grades(BaseModel):
    math: int
    science: int
    history: int
    english: int

class Students(BaseModel):
    student_id: str
    name: str
    grades: Grades
    
class MissedDays(BaseModel):
    student_id: str
    missed_days: int  



