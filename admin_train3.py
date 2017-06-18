import psycopg2
import sys
from geopy.distance import vincenty
import pyrenn
import numpy
from datetime import datetime

lista_puntos = []
lista_recorridos = []
bRecorridoV = False
lista_puntosV = []
lista_puntosI = []
Last_Recorrido = "0"
Cont_Recorrido = 0
Cont_Recorrido_Total = 0
fin_global = []

def conect():
    conn_string ="host='127.0.0.1' port='5432' dbname='admingeso' user='postgres' password='root'"

    print ("Conectando a: \n =>%s" % (conn_string) )
    conn = psycopg2.connect(conn_string)
    print ("Conectado!")
    return conn

def parser_time(dato):
    spliter = dato[2].split(' ')
    splitFecha = spliter[0].split('/')
    splitHora = spliter[1].split(':')
    dia = float(splitFecha[0])/31
    horaStand = (float(splitHora[0]))/(24)
    minStand = (float(splitHora[1]))/(60)
    segStand = (float(splitHora[2]))/60
    datoStand = [dia,horaStand,minStand,segStand,dato[2]]
    #print (datoStand)
    return datoStand

def neuralCreation(datos, results):
    nn = pyrenn.CreateNN([6,10,10,5,1])
    tras = (numpy.array(datos)).transpose(1, 0)
    res = numpy.array(results)
    num_max = len(results)
    return pyrenn.train_LM(tras, res, nn, verbose=True,k_max=300,E_stop=1e-5)
    
def agregar_recorrido():
    celda_recorrido = []
    puntos_r = lista_puntos[:]
    celda_recorrido.append(Last_Recorrido)
    #print str(bRecorridoV)+" AGREGANDO RECORRIDO"

    if(bRecorridoV == True):    
        celda_recorrido.append("V")
    else:
        celda_recorrido.append("I")

    celda_recorrido.append(Cont_Recorrido)
    celda_recorrido.append(puntos_r)
    
    lista_recorridos.append(celda_recorrido)
    
    #print (puntos_r)
    
    del lista_puntos[:]
    
    #print(lista_recorridos[len(lista_recorridos)-1][3])
    
def iterarDatos(datos):
    global Last_Recorrido
    global Cont_Recorrido
    global Cont_Recorrido_Total
    global bRecorridoV
    global lista_puntosV
    global lista_puntosI
    global lista_puntos
    lista_puntosV = []
    cont_print = 0
    for record in datos:
        
    #Lista_recorridos[ Numero de patente, I/V , Num_Recorrido , Lista_Puntos[ Lista_Fecha,Lista_Hora,LatLong ]
        if(Last_Recorrido == "0"):
            if(record[4] == "V"):
                bRecorridoV = True
            else:
                bRecorridoV = False
            Last_Recorrido = record[1]
        
        #Cambio de recorrido
        
        if(Last_Recorrido != record[1]):
                
            #Agregamos el recorrido anterior a la lista de recorridos
            agregar_recorrido()
            
            #Inicializamos la ruta en funcion de la nueva
            
            if(record[4] == "V"):
                bRecorridoV = True
            else:
                bRecorridoV = False
            Last_Recorrido = record[1]
            Cont_Recorrido = 0
            Cont_Recorrido_Total += 1
        
            #print ("\n\n\n\n\n\n\n\n\n\n\n\n\n")
            #print ("---- Cambio de patente detectado. Procediendo a reiniciar contadores ----")
            #print ("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
        
        #Recreamos el cambio de ruta
        if(record[4] == "V" and bRecorridoV == False):
            
            cont_print += len(lista_puntos)
            agregar_recorrido()
            bRecorridoV = True
            Cont_Recorrido += 1
            Cont_Recorrido_Total += 1
            #print ("Cambio de recorrido detectado, Recorrido en V")
            #print("Cantidad de recorridos de la patente "+ str(Cont_Recorrido))
            #print("Cantidad de recorridos totales "+ str(Cont_Recorrido_Total))
            
        
        elif(record[4] == "I" and bRecorridoV == True):
            #agregar_recorrido()
            del lista_puntos[:]
            bRecorridoV = False
            #Cont_Recorrido += 1
            #Cont_Recorrido_Total += 1
            #print ("Cambio de recorrido detectado, Recorrido en I")
            #print("Cantidad de recorridos de la patente "+ str(Cont_Recorrido))
            #print("Cantidad de recorridos totales "+ str(Cont_Recorrido_Total))
        
        celda = []
        #celda.append(parser_date(record))
        
        tiempo = parser_time(record)
        celda.append(tiempo[4])
        celda.append(tiempo[0])
        celda.append(tiempo[1])
        celda.append(tiempo[2])
        celda.append(tiempo[3])
        celda.append(float(record[5].replace(",", ".")))
        celda.append(float(record[6].replace(",", ".")))
        
        #celda.append(Cont_Recorrido_Total)
        
        #if(cont_print % 40 == 0):
        #    print (celda)
         #   print record[4]
            
        lista_puntos.append(celda)
        
        if(bRecorridoV == True):
            lista_puntosV.append(celda)
        else:
            lista_puntosI.append(celda)

            
    #Hacer la ultima operacion para la ultima ruta
    #Cont_Recorrido += 1
    #Cont_Recorrido_Total += 1
    #agregar_recorrido()
    print("Cantidad de puntos "+ str(cont_print))
    print("Cantidad de recorridos de la patente "+ str(Cont_Recorrido))
    print("Cantidad de recorridos totales "+ str(Cont_Recorrido_Total))

def prepTrain():
    matriz = []
    salidas = []
    for reco in lista_recorridos:
        dis = 0
        cont = 0
        for punto in reco[3]:
            if(cont<(len(reco[3])-1)):
                dato = []
                #print(punto[0])
                t1 = datetime.strptime(punto[0],"%d/%m/%Y %H:%M:%S")
                t2 = datetime.strptime(reco[3][cont+1][0],"%d/%m/%Y %H:%M:%S")
                dt = t2-t1
                dato.append(punto[1])
                dato.append(punto[2])
                dato.append(punto[3])
                dato.append(punto[4])
                dato.append(dis)
                dato.append(vincenty((punto[5],punto[6]),(reco[3][cont+1][5],reco[3][cont+1][6])).meters)
                matriz.append(dato)
                dis += vincenty((punto[4],punto[5]),(reco[3][cont+1][4],reco[3][cont+1][5])).meters
                #print (dt.seconds)
                result = (dt.seconds)
                salidas.append(result)
            cont += 1
    return [matriz,salidas]
        
def main():
    global Last_Recorrido
    global Cont_Recorrido
    global Cont_Recorrido_Total
    global bRecorridoV
    global lista_puntosV
    global lista_puntosI
    global lista_puntos
    global fin_global
    
    conn = conect()
    cursor = conn.cursor()
    query = "SELECT * FROM datos LIMIT 23000"
    cursor.execute(query)
    records = cursor.fetchall()
    iterarDatos(records)
    conn.close()
    
    
    print ("Conexion Cerrada")
    print(len(lista_recorridos))
    print ("Voy a preparar")
    fin = prepTrain()
    fin_global = fin[:]
    print ("Prepare")
    print("Salidas: "+str(fin[1][0])+" Entradas:"+str(fin[0][0]))
    nn = neuralCreation(fin[0], fin[1])
    pyrenn.saveNN(nn,'rnn_create3.csv')
if __name__ == "__main__":
    main()
    
