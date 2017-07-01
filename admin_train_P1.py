import psycopg2
import sys
from geopy.distance import vincenty
import pyrenn
import numpy
import csv
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
    spliter = dato[1].split(' ')
    splitFecha = spliter[0].split('/')
    splitHora = spliter[1].split(':')
    dia = float(splitFecha[0])/31
    horaStand = (float(splitHora[0]))/(24)
    minStand = (float(splitHora[1]))/(60)
    segStand = (float(splitHora[2]))/60
    datoStand = [dia,horaStand,minStand,segStand,dato[1]]
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
            if(record[3] == "V"):
                bRecorridoV = True
            else:
                bRecorridoV = False
            Last_Recorrido = record[0]
        
        #Cambio de recorrido
        
        if(Last_Recorrido != record[0]):
                
            #Agregamos el recorrido anterior a la lista de recorridos si no es vacia
            #print len(lista_puntos)
            if len(lista_puntos) > 20:
                agregar_recorrido()
                Cont_Recorrido_Total += 1
            else:
                del lista_puntos[:]
            
            #Inicializamos la ruta en funcion de la nueva
            
            if(record[3] == "V"):
                bRecorridoV = True
            else:
                bRecorridoV = False
            Last_Recorrido = record[0]
            Cont_Recorrido = 0
        
            #print ("\n\n\n\n\n\n\n\n\n\n\n\n\n")
            #print ("---- Cambio de patente detectado. Procediendo a reiniciar contadores ----")
            #print ("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
        
        #Recreamos el cambio de ruta
        if(record[3] == "V" and bRecorridoV == False):
            
            cont_print += len(lista_puntos)
            agregar_recorrido()
            bRecorridoV = True
            Cont_Recorrido += 1
            Cont_Recorrido_Total += 1
            #print ("Cambio de recorrido detectado, Recorrido en V")
            #print("Cantidad de recorridos de la patente "+ str(Cont_Recorrido))
            #print("Cantidad de recorridos totales "+ str(Cont_Recorrido_Total))
            
        
        elif(record[3] == "I" and bRecorridoV == True):
            #agregar_recorrido()
            del lista_puntos[:]
            bRecorridoV = False
            #Cont_Recorrido += 1
            #Cont_Recorrido_Total += 1
            #print ("Cambio de recorrido detectado, Recorrido en I")
            #print("Cantidad de recorridos de la patente "+ str(Cont_Recorrido))
            #print("Cantidad de recorridos totales "+ str(Cont_Recorrido_Total))
        
        celda = []
        
        tiempo = parser_time(record)
        #DIA
        celda.append(tiempo[0])
        #HORA
        celda.append(tiempo[1])
        #MINUTO
        celda.append(tiempo[2])
        #SEGUNDO
        celda.append(tiempo[3])
        
        celda.append(float(record[4].replace(",", ".")))
        celda.append(float(record[5].replace(",", ".")))

        celda.append(tiempo[4])
        #celda.append(parser_date(record))
        
        #celda.append(Cont_Recorrido_Total)
        
        if(cont_print % 10000 == 0):
            print (celda)
            
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

    return lista_recorridos

def filtrarDatos(datos):
    datos = datos[:]
    print datos[0]
    cantidad_27 = 0
    cantidad_24 = 0
    pos_ruta = 0

    promedio_km = 0.0
    cantidad_rutas = 0

    for reco in datos:
        dis = 0
        cont = 0
        for punto in reco[3]:
            if(cont<(len(reco[3])-1)):
                dis += vincenty((punto[4],punto[5]),(reco[3][cont+1][4],reco[3][cont+1][5])).meters
            cont += 1

        if(dis > 27000):
            cantidad_27 += 1
            del datos[pos_ruta]
            continue

        elif(dis < 26000):
            cantidad_24 += 1
            del datos[pos_ruta]
            continue

        promedio_km += dis
        cantidad_rutas += 1
        pos_ruta += 1

    #promedio_km = promedio_km / float(cantidad_rutas)
    #print "Promedio de km de las rutas "+ str(promedio_km) 
    #print ("Cantida de rutas detectadas sobre 27K = "+ str(cantidad_27))
    #print ("Cantida de rutas detectadas bajo 26K = "+ str(cantidad_24))
    return datos

def prepTrain(l_recorridos_filtrados):
    matriz = []
    salidas = []

    #l_recorridos_filtrados es la lista con los recorridos filtrados, listos para ser ingresados a la red

    #Reco tiene los siguientes parametros:
    '''
    reco[0] = recorrido de la ruta 
    reco[1] = IDA o VUELTA de la ruta 
    reco[2] = ID de la ruta (es la N ID ruta de dicha patente)
    reco[3] = lista de listas de puntos latlong. Aqui van todos los puntos de dicho recorrido

    '''

    for reco in l_recorridos_filtrados:
        #print reco
        dis = 0

        #OJO: Se parte de 1 ya  que la recurrencia necesita de un punto anterior
        cont = 1
        for punto in reco[3]:

            '''
            Punto tiene los siguientes parametros:

            punto[0] DIA 
            punto[1] HORA 
            punto[2] MINUTO
            punto[3] SEGUNDO
            punto[4] LAT , QUE ES IGUAL A reco[3][cont][4]
            punto[5] LONG , QUE ES IGUAL A reco[3][cont][5]

            '''

            if(cont<(len(reco[3])-1)):
                dato = []
                #print(punto[0])
                t1 = datetime.strptime(reco[3][cont-1][6],"%d/%m/%Y %H:%M:%S")
                t2 = datetime.strptime(reco[3][cont][6],"%d/%m/%Y %H:%M:%S")
                dt = t2-t1
                dato.append(punto[1])
                dato.append(punto[2])
                dato.append(punto[3])
                dato.append(punto[4])



                #print cont
                #print "-------ANTERIOR----------"
                #POS ANTERIOR
                dato.append(dis)

                #print dis

                #POS ACTUAL
                #print "-------ACTUAL---------"
                #print "VICENTY "+str(vincenty((reco[3][cont-1][4],reco[3][cont-1][5]),(reco[3][cont][4],reco[3][cont][5])).meters)
                #print "VICENTY DIVIDIDO "+str(vincenty((reco[3][cont-1][4],reco[3][cont-1][5]),(reco[3][cont][4],reco[3][cont][5])).meters/27000)
                
                #VICENTY TOMA EL PUNTO ANTERIOR Y EL PUNTO ACTUAL PARA OBTENER CUANTOS METROS HAY, Y LOS DIVIDE POR 27K
                dis += vincenty((reco[3][cont-1][4],reco[3][cont-1][5]),(reco[3][cont][4],reco[3][cont][5])).meters/27500
                dato.append(dis)

                #print dis

                #dato.append(dis)
                #dato.append(vincenty((reco[3][cont-1][4],reco[3][cont-1][5]),(punto[4],punto[5])).meters/27000.0)
                matriz.append(dato)
                #print (dt.seconds)
                result = (dt.seconds)
                print result
                salidas.append(result)
                #if(dis > 28000):
                    #print dis
                    #print "DETECTADA RUTA CON SOBRE 28KM DE DISTANCIA"
            cont += 1
        print "---------- FIN"
    return [matriz,salidas]

def leer_csv():
    celdas_totales_csv = []
    with open("1-10-Janeiro-Belem-out.csv", 'rb') as f:
            reader = csv.reader(f)
            your_list = list(reader)
            celdas_totales_csv.extend(your_list)
    return celdas_totales_csv

def leer_todos_csv():
    lista_archivos = []
    celdas_totales_csv = []

    #Lista de archivos a leer
    archivos = open("nombres_csv_procesar.txt",'r')

    for archivo in archivos:
        lista_archivos.append(archivo)
    archivos.close()

    for archivo_leer in lista_archivos:
        with open(archivo_leer.replace("\n", "").replace(".xlsx","-out.csv"), 'rb') as f:
            reader = csv.reader(f)
            your_list = list(reader)
            celdas_totales_csv.extend(your_list)
    return celdas_totales_csv   
        
def main():
    global Last_Recorrido
    global Cont_Recorrido
    global Cont_Recorrido_Total
    global bRecorridoV
    global lista_puntosV
    global lista_puntosI
    global lista_puntos
    global fin_global
    
    #conn = conect()
    #cursor = conn.cursor()
    #query = "SELECT * FROM datos LIMIT 23000000"
    #cursor.execute(query)
    #records = cursor.fetchall()
    records = leer_todos_csv()
    print "Numero de puntos a procesar: "+str(len(records))
    print records[0]
    
    l_recorridos = iterarDatos(records)
    l_recorridos_filtrados = filtrarDatos(l_recorridos)
    #conn.close()
    
    
    print ("Conexion Cerrada")
    print ("Lista de recorridos anteior")
    print(len(lista_recorridos))
    print ("Lista de recorridos filtrada")
    print (len(l_recorridos_filtrados))
    print ("Voy a preparar")
    fin = prepTrain(l_recorridos_filtrados)
    fin_global = fin[:]
    print ("Prepare")
    print("Salidas: "+str(fin[1][0])+" Entradas:"+str(fin[0][0]))
    nn = neuralCreation(fin[0], fin[1])
    pyrenn.saveNN(nn,'rnn_create.csv')
if __name__ == "__main__":
    main()
    
