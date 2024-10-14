#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import requests
from io import BytesIO
import config


# In[ ]:


# функция добавляет ИД документа к базовому УРЛ гугла
# функция, чтобы забрать из ссылки гугл докс ИД
# ИД находится между определенными паттернами
# на выходе возвращаем строку с ИД
def get_data_from_sheet(base_link):
    # # стандартный УРЛ от гугла, в который нужно подставить ИД файла для скачивания
    url = config.url #'https://docs.google.com/spreadsheets/export?exportFormat=xlsx&id='
    start_index = str(base_link).find('/d/')
    end_index = str(base_link).find('/edit?')
    spreadsheetId = base_link[start_index+3:end_index]
    
    # spreadsheetId = get_sheet_id(base_link)
    # добавляем к стандартной ссылке гугл этот ИД
    final_url = url + spreadsheetId
    res = requests.get(final_url)
    
    return BytesIO(res.content)


# In[ ]:


# Функция для парсинга Чек Листов 
# принимает лист гугл документа, на котором находится основная статистика (Чек-лист) и парсит его
# на выходе возвращает датаФрейм
# на вход принимает:
# content - данные с листа гугл документа
# target_sheet - название листа
# name - название клиента
def parse_check_list_report(content, target_sheet, name, report):
    # создаем датаФрейм на основании данных с листа гугл документа
    df_tmp = pd.read_excel(content, sheet_name=target_sheet)
    df_tmp = df_tmp.fillna('')
    # формируем заголовки - приводим в нижний регистр, обрезаем по краям, избаляемся от технических символов(перенос строки и тд)
    df_tmp.columns = df_tmp.iloc[0].str.lower().str.strip().str.replace('\n', ' ') # забираем название полей из файла
    # забираем индекс строки с весом вопроса
    weight_index = list(df_tmp[df_tmp['фио менеджера'].str.lower()=='вес вопроса'].index)[0] # берем индекс окончания таблицы с данными
    # создаем рндекс начала основной таблицы
    start_index = weight_index + 1
     # забираем строку, которая содержит вес показателей (в отдельный датаФрейм)
    df_weight = df_tmp.iloc[weight_index:start_index]
    # df_weight = df_tmp.iloc[3:4]
    # формируем основную таблицу (убираем все технические строки, которые идут ДО данных)
    df_tmp = df_tmp.iloc[start_index:]
    # в некоторых случаях вместо поля Дата звонка встречается название Дата переписки
    if 'дата переписки' in list(df_tmp.columns):
        # убираем строки, в которых не заполнена дата
        df_tmp = df_tmp[df_tmp['дата переписки']!='']
        # приводим дату к обычному виду
        df_tmp['дата переписки'] = pd.to_datetime(df_tmp['дата переписки']).dt.date # приводим в формат даты
    else:
        # убираем строки, в которых не заполнена дата
        df_tmp = df_tmp[df_tmp['дата звонка']!='']
    # df_tmp = df_tmp.iloc[start_index:]
        # приводим дату к обычному виду
        df_tmp['дата звонка'] = pd.to_datetime(df_tmp['дата звонка']).dt.date # приводим в формат даты
    df_tmp = df_tmp.reset_index(drop=True)
    # объединяем датаФрейм Вес и основные данные
    # т.к. оба датаФрейма одинаковые по структуре, получается, что ПОД Вес мы ставим основные данные
    final_df = pd.concat([df_weight, df_tmp])
    # добавляем название клиента
    final_df['client'] = name
    # отчеты Калиниград - каждый отчет в отдельном гугл доксе. Поэтому название формируем из названия отчета, который мы пробросили
    if 'калининград' in name.lower():
        dashboard_name = report
    # в остальных клиентах все отчеты с данными находятся в одном гугл доксе на разных листах
    # поэтому передаем название листа
    else:
        dashboard_name = target_sheet.lower()
    final_df['dashboard'] = dashboard_name

    return final_df


# In[4]:


# Функция для парсинга CRM 
# принимает лист гугл документа, на котором находится основная статистика CRM и парсит его
# на выходе возвращает датаФрейм
# на вход принимает:
# content - данные с листа гугл документа
# target_sheet - название листа
# name - название клиента
def parse_crm_report(content, target_sheet, name, report):
    # создаем датаФрейм на основании данных с листа гугл документа
    df_tmp = pd.read_excel(content, sheet_name=target_sheet, header=None)
    df_tmp = df_tmp.fillna('')
    df_tmp.columns = df_tmp.iloc[0].str.lower().str.strip().str.replace('\n', ' ') # забираем название полей из файла
    df_tmp = df_tmp.iloc[1:]
    df_tmp = df_tmp[df_tmp['дата']!='']
    df_tmp['дата'] = pd.to_datetime(df_tmp['дата']).dt.date # приводим в формат даты
    df_tmp = df_tmp.reset_index(drop=True)
    df_tmp['client'] = name
    df_tmp['dashboard'] = report
        
    return df_tmp


# In[ ]:


# Функция для парсинга списка сотрудников 
# принимает лист гугл документа, на котором находится основная статистика CRM и парсит его
# на выходе возвращает датаФрейм
# на вход принимает:
# content - данные с листа гугл документа
# target_sheet - название листа
# name - название клиента
def parse_employees_report(content, target_sheet, name, report):
    # создаем датаФрейм на основании данных с листа гугл документа
    df_tmp = pd.read_excel(content, sheet_name=target_sheet, header=None)
    df_tmp = df_tmp.fillna('')
    df_tmp.columns = df_tmp.iloc[0].str.lower().str.strip().str.replace('\n', ' ') # забираем название полей из файла
    df_tmp = df_tmp.iloc[1:]
    df_tmp = df_tmp[df_tmp['фио']!='']
    df_tmp = df_tmp.reset_index(drop=True)
    df_tmp['client'] = name
    df_tmp['dashboard'] = report
        
    return df_tmp


# In[ ]:


def parse_targets_plan_report(content, target_sheet, name, report):
    df_tmp = pd.read_excel(content, sheet_name=target_sheet, header=None)
    df_tmp = df_tmp.fillna('')
    df_tmp.columns = df_tmp.iloc[0].str.lower().str.strip().str.replace('\n', ' ') # забираем название полей из файла
    df_tmp = df_tmp.iloc[1:]
    
    if target_sheet.lower() != 'справочник':
        df_tmp = df_tmp[df_tmp['дата начала']!='']
        df_tmp['дата начала'] = pd.to_datetime(df_tmp['дата начала']).dt.date # приводим в формат даты
        df_tmp['дата окончания'] = pd.to_datetime(df_tmp['дата окончания']).dt.date # приводим в формат даты
        df_tmp = df_tmp.reset_index(drop=True)
        df_tmp['client'] = name
        df_tmp['dashboard'] = report

    return df_tmp

