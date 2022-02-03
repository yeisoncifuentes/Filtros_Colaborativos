# -*- coding: utf-8 -*-
"""
Created on Fri Jan 21 20:23:27 2022

@author: Independence
"""


import pandas as pd
import pyodbc

from math import sqrt

cnxn = pyodbc.connect('DRIVER={PostgreSQL ANSI};Server=kashin.db.elephantsql.com;Port=5432;Database=kkvjzwdv;UID=kkvjzwdv;Password=Ro1Q2gWS-quabDjVL71_ZSsyAuXrjOt_')


def querys():
    
    query_planes = '''SELECT plan_id,nombre
                    FROM oferta_turistica.planes
                    '''
                    
    query_puntuaciones = '''SELECT calificacion,turista_id,plan_id
                    FROM recomendacion.puntuaciones_planes
                    '''
                    
    planes_df = pd.read_sql(query_planes, cnxn)
    puntuaciones_df=pd.read_sql(query_puntuaciones, cnxn)
    
    return planes_df,puntuaciones_df
    

                
def getCalifications(idUser):
    
     query_get_user = '''SELECT usuario_nuevo,intereses
                        FROM usuario.turistas WHERE id= {}
                    '''.format(idUser)  
     turista_df= pd.read_sql(query_get_user, cnxn)
     turista=turista_df.usuario_nuevo[0]
     if(turista=='0'):
         query_calificacion = '''SELECT plan_id,calificacion
                        FROM recomendacion.puntuaciones_planes WHERE turista_id= '{}'
                    '''.format(idUser)
         calificaciones_df = pd.read_sql(query_calificacion, cnxn)
     else:         
         calificaciones_df=pd.read_json(turista_df.intereses[0])
                    
                    
     return calificaciones_df
        

def logic_recomendation(id_user):       
        
    inputPlanes=getCalifications(id_user)
    
    
    planes_df,puntuaciones_df = querys()
   
    
    userSubset = puntuaciones_df[puntuaciones_df['plan_id'].isin(inputPlanes['plan_id'].tolist())]
    
    
    userSubsetGroup = userSubset.groupby(['turista_id'])
    userSubsetGroup = sorted(userSubsetGroup,  key=lambda x: len(x[1]), reverse=True)
    
    
    #Guardar la Correlación Pearson en un diccionario, donde la clave es el Id del usuario y el valor es el coeficiente
    pearsonCorrelationDict = {}
    
    #Para cada grupo de usuarios en nuestro subconjunto 
    for name, group in userSubsetGroup:
        #Comencemos ordenando el usuario actual y el ingresado de forma tal que los valores no se mezclen luego
        group = group.sort_values(by='plan_id')
        inputPlanes = inputPlanes.sort_values(by='plan_id')
        #Obtener el N para la fórmula
        nCalificacion = len(group)
        #Obtener los puntajes de revisión para las películas en común
        temp_df = inputPlanes[inputPlanes['plan_id'].isin(group['plan_id'].tolist())]
        #Guardarlas en una variable temporal con formato de lista para facilitar cálculos futuros
        tempRatingList = temp_df['calificacion'].tolist()
        #Pongamos también las revisiones de grupos de usuarios en una lista
        tempGroupList = group['calificacion'].tolist()
        #Calculemos la Correlación Pearson entre dos usuarios, x e y
        Sxx = sum([i**2 for i in tempRatingList]) - pow(sum(tempRatingList),2)/float(nCalificacion)
        Syy = sum([i**2 for i in tempGroupList]) - pow(sum(tempGroupList),2)/float(nCalificacion)
        Sxy = sum( i*j for i, j in zip(tempRatingList, tempGroupList)) - sum(tempRatingList)*sum(tempGroupList)/float(nCalificacion)
    
        #Si el denominador es diferente a cero, entonces dividir, sino, la correlación es 0.
        if Sxx != 0 and Syy != 0:
            pearsonCorrelationDict[name] = Sxy/sqrt(Sxx*Syy)
        else:
            pearsonCorrelationDict[name] = 0
    
    
    
    
    pearsonDF = pd.DataFrame.from_dict(pearsonCorrelationDict, orient='index')
    pearsonDF.columns = ['similarityIndex']
    pearsonDF['turista_id'] = pearsonDF.index
    pearsonDF.index = range(len(pearsonDF))
    
    
    topUsers=pearsonDF.sort_values(by='similarityIndex', ascending=False)[0:50]
    
    topUsersRating=topUsers.merge(puntuaciones_df, left_on='turista_id', right_on='turista_id', how='inner')
    
    #Se multiplica la similitud de los puntajes de los usuarios
    topUsersRating['weightedRating'] = topUsersRating['similarityIndex']*topUsersRating['calificacion']
    
    #Se aplica una suma a los topUsers luego de agruparlos por userId
    tempTopUsersRating = topUsersRating.groupby('plan_id').sum()[['similarityIndex','weightedRating']]
    tempTopUsersRating.columns = ['sum_similarityIndex','sum_weightedRating']
    
    #Se crea un dataframe vacío
    recommendation_df = pd.DataFrame()
    #Ahora se toma el promedio ponderado
    recommendation_df['weighted average recommendation score'] = tempTopUsersRating['sum_weightedRating']/tempTopUsersRating['sum_similarityIndex']
    recommendation_df['plan_id'] = tempTopUsersRating.index
    
    #Se crea un dataframe vacío
    recommendation_df = pd.DataFrame()
    #Ahora se toma el promedio ponderado
    recommendation_df['weighted average recommendation score'] = tempTopUsersRating['sum_weightedRating']/tempTopUsersRating['sum_similarityIndex']
    recommendation_df['turista_id'] = tempTopUsersRating.index
    
    recommendation_df = recommendation_df.sort_values(by='weighted average recommendation score', ascending=False)
    
    
    recomendation_final=planes_df.loc[planes_df['plan_id'].isin(recommendation_df.head(10)['turista_id'].tolist())]
    return recomendation_final



