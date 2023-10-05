#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 30 15:56:07 2023

@author: yoba
"""

import pandas as pd
import sqlite3
import os
import json
import glob

directory=r'/home/yoba/Projects/education4climate/education4climate/data/crawling-output'
scoring_directory=r'/home/yoba/Projects/education4climate/education4climate/data/scoring-output'
patterns_directory=r'/home/yoba/Projects/education4climate/education4climate/data/patterns/base'


db_file=r'/home/yoba/Projects/education4climate/open-data/output/education4climate.sqlite'

os.remove(db_file)
db=sqlite3.connect(db_file)

def loadProgram(year,entity):
    print(f'Entity-Program: {year}-{entity}')
    
    program=pd.read_json(f'{directory}/{entity}_programs_{year}.json')
    program['entity']=entity
    program['year']=year
    
    if 'ects' not in program.columns:
        print('No ECTS')
        program['ects']=None
    
    #program[['id','courses','ects']].explode(['courses','ects']).to_sql('R_program_courses',db,if_exists='append',index=False)
    program[['entity','year','id','courses']].explode(['courses']).rename(columns={'id':'id_program','courses':'id_course'}).to_sql('R_program_courses',db,if_exists='append',index=False)

    keep=[
            'entity',
            'year',
            'id',
            'name',
            'cycle',
            'url'
        ]
    keep=[c for c in keep if c in program.columns]

    program[keep].rename(columns={'id':'id_program'}).to_sql('T_program',db,if_exists='append',index=False)

    if 'faculties'  in program.columns:
        program[['entity','year','id','faculties']].explode(['faculties']).rename(columns={'id':'id_program'}).to_sql('R_program_faculties',db,if_exists='append',index=False)

    if 'campuses'  in program.columns:
        program[['entity','year','id','campuses']].explode(['campuses']).rename(columns={'id':'id_program'}).to_sql('R_program_campuses',db,if_exists='append',index=False)

def loadCourse(year,entity):
    
    print(f'Entity-Courses: {year}-{entity}')

    courses=pd.read_json(f'{directory}/{entity}_courses_{year}.json')
    courses['entity']=entity
    courses['year']=year
    
    courses[['entity','year','id','teachers']].explode('teachers').rename(columns={'id':'id_course'}).to_sql('R_courses_teachers',db,if_exists='append',index=False)
    courses[['entity','year','id','languages']].explode('languages').rename(columns={'id':'id_course'}).to_sql('R_courses_languages',db,if_exists='append',index=False)
    keep=[
            'entity',
            'year',
            'id',
            'activity',
            'content',
            'goal',
            'name',
            'other',
            'url'
        ]
    keep=[c for c in keep if c in courses.columns]
    
    courses[keep].rename(columns={'id':'id_course'}).to_sql('T_courses',db,if_exists='append',index=False)
    
    pd.DataFrame({'entity':entity,'year':year,'file':'courses','column':courses.columns}).to_sql('R_courses_columns',db,if_exists='append',index=False)


def loadScoringResults(year,entity):
    # Scoring results
            
    print(f'Scoring: {year}-{entity}')

    scoring=pd.read_csv(f'{scoring_directory}/{entity}_courses_scoring_{year}.csv',sep=',')
    scoring['entity']=entity
    scoring['year']=year
    
    scoring.rename(columns={'id':'id_course'}).to_sql('T_courses_scoring_results',db,if_exists='append',index=False)


def loadMatches(year,entity):
    # Matches
            
    print(f'Matching: {year}-{entity}')


    with open(f'{scoring_directory}/{entity}_matches_{year}.json','r') as f:
        matches=json.load(f)
    
    matching=[]
    for course in matches.keys():
        for language in matches[course].keys():
            for pattern in matches[course][language].keys():
                for match in matches[course][language][pattern]:
                    id_course=course.split(':')[0]
                    matching.append((year, entity, id_course,language,pattern,match))
                    
    matching=pd.DataFrame(matching,columns=['year','entity','id_course','language','pattern','match'])
    matching.to_sql('T_courses_scoring_pattern_matches',db,if_exists='append',index=False)


files=[ file[0:-5].split('_') for file in os.listdir(directory)]
files=[ e for e in files if len(e)==3]

for (entity, operation, year) in files:
    if operation=='programs':
        loadProgram(year,entity)
    if operation=='courses':
        loadCourse(year,entity)


os.chdir(scoring_directory)


scoringResultsFiles=[ file[0:-4].split('_') for file in glob.glob('*_courses_scoring_*.csv')]
for (entity, _, _, year) in scoringResultsFiles:
    # ATTENTION: FILTRE
    if year in [2021,2022]:
        loadScoringResults(year,entity)
    
matchesFiles=[ file[0:-5].split('_') for file in glob.glob('*_matches_*.json')]
for (entity, _, year) in matchesFiles:    
    loadMatches(year, entity)
         
    
for language in ['fr','nl','en']: 
    # Patterns    
    print(f'Patterns to themes: {language}')
    patterns2themes=pd.read_csv(f'{patterns_directory}/{language}.csv',sep=',')
    patterns2themes.themes=patterns2themes.apply(lambda row: eval(row.themes),axis=1)
    patterns2themes=patterns2themes.explode('themes')
    patterns2themes['language']=language
    patterns2themes=patterns2themes.rename(columns={'patterns':'pattern','themes':'theme'})
    patterns2themes.to_sql('T_courses_scoring_pattern_themes',db,if_exists='append',index=False)

    
db.close()
