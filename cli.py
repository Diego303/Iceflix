#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import Ice
import hashlib
import time
from threading import Timer
import getpass

try:
    Ice.loadSlice('iceflix.ice')
    import IceFlix
except ImportError:
    Ice.loadSlice(os.path.join(os.path.dirname(__file__), "iceflix.ice"))
    import IceFlix

class Client(Ice.Application):

    def renovar_login(self, user, password_user, servicioAuth ,token_list ,timers , current=None):
        try:
            token = servicioAuth.refreshAuthorization(user,hashlib.sha256(password_user.encode()).hexdigest())
            token_list[0] = token
            timer = Timer(100.0,self.renovar_login,(user, password_user, servicioAuth, token_list, timers))
            timers[0]=timer
            timer.start()
            
        except Exception as ex:
            print('\n Lo Sentimos, ha ocurrido un error inesperado!.\nSe cerrara la sesion de su Usuario. \n')
            token_list[0]=''
    
    def seleccionar_busqueda(self, busqueda_catalog, servicioCatalog, current=None):
        print('Seleccione un Titulo de su busqueda anterior:')
        cont=1
        diccionario_ids={}
        try:
            for media_id in busqueda_catalog:
                media_result = servicioCatalog.getTile(media_id,'anonimo')
                print(" "+str(cont) +"."+ media_result.info.name)
                diccionario_ids[str(cont)]=media_result.mediaId
                cont=cont+1
                
            titulo_seleccionado = input('Introduzca el número del título >')
            titulo_seleccionado = diccionario_ids[titulo_seleccionado]
        except Exception as ex:
            print('Seleccion Incorrecta.')
            return None
            
        return titulo_seleccionado
            

    def run(self, argv):
        seguir=True
        conexion_establecida=False
        intentosTotales=3
        user=''
        autentificado=False
        token_list=['']
        timers=[None]
        timer=None
        busqueda_catalog=[]
        
        while seguir==True:
            if conexion_establecida==False:
                print(" ### Bienvenido a Iceflix ### \n")
                print("1.Conectarse a un servidor")
                print("2.Definir Intentos para Reconexion")
                print("3.Cerrar Programa")
                opcion = input("Selecciones una de las opciones [1,2,3]: ")
            
                if opcion=='1':
                    mainServerProxy = input(" Introduzca Proxy del Servidor: \n")
                    
                    intentos=intentosTotales
                    while intentos>0 and conexion_establecida==False:
                        try:
                            proxy = self.communicator().stringToProxy(mainServerProxy)
                            main = IceFlix.MainPrx.checkedCast(proxy)
                            if not main:
                                raise RuntimeError('Invalid proxy')
                        
                            print("\nConectado a Servidor Correctamente. \n")
                            conexion_establecida=True
                        
                        except Exception as ex:
                            print("Conexion fallida. Reintentando...")
                            time.sleep(5)
                            intentos = intentos-1
                              
                    if intentos==0:
                        print('\n Proxy del Server Incorrecto o Server No Disponible. \n')
                          
                if opcion=='2':
                    intentosTotales = input("Introduce el numero de intentos (Actuales "+str(intentosTotales)+"):")
                    intentosTotales = int(intentosTotales)
                    print('\n')
                
                if opcion=='3':
                    print("\nGracias por usar nuestro programa!! \n")
                    seguir=False
            
                if opcion!='1'and opcion!='2' and opcion!=3:
                    None
            
            comando=''
            print("\n -------------------------------------------------------- \n")
            
            if autentificado and conexion_establecida and token_list[0]!='':
                print('Estado de Sesión: Usuario '+user+' esta Logueado. \n')
            elif token_list[0]=='' and conexion_establecida:
                print('Estado de Sesión: Usuario Anonimo. \n')
            
            if conexion_establecida:
                print("Introduzca el Servicio que desee utilizar o si necesita ayuda use 'help'")
                comando=input(">")
                
            if comando=='help':
                print("\nServicios Disponibles:\n\tauthen -> Authenticarte en el sistema o administrar usuarios. "+
                    "\n\tcatalog -> Realizar búsquedas en el Catalogo de IceFlix "+
                    "o Administrar Tags" +
                    "\n\n\texit -> Cerrar el Programa" + 
                    "\n\thelp -> Proporciona ayuda de los servicios disponibles \n")
            
            if comando=='exit':
                try:
                    timers[0].cancel()
                except Exception as ex:
                    None
                print("\nGracias por usar nuestro programa!! \n")
                seguir=False
                
            if comando=='authen':
                authenticator_conectado=False
                try:
                    servicioAuth = main.getAuthenticator()
                    authenticator_conectado = True
                    
                    print("Servicio de Authentificacion:\n"+
                "\t login -> Authenticarte con usuario y contraseña \n" +
                "\t logout -> Cerrar Sesión \n\n" +
                "Funciones Administrativas\n" +
                "\t add -> Añadir Usuario \n" +
                "\t remove -> Borrar Usuario \n")
                    comando=input('Introduzca servicio deseado >')
                
                except IceFlix.TemporaryUnavailable as ex:
                    print('Lo sentimos, este servicio no esta disponible actualmente.')
                
                
                
                if comando=='login' and authenticator_conectado:
                    user=input('Nombre de Usuario:')
                    password_user = getpass.getpass('Contraseña:')
                    try:
                        token = servicioAuth.refreshAuthorization(user,hashlib.sha256(password_user.encode()).hexdigest())
                        token_list[0] = token
                        print('Usuario Correcto! La sesion ha sido Iniciada. \n')
                        autentificado=True
                        timer = Timer(100.0,self.renovar_login,(user, password_user, servicioAuth, token_list, timers))
                        timers[0] = timer
                        timer.start()
                        
                    except IceFlix.Unauthorized as ex:
                        print('Usuario o Contraseña Incorrectos.\n')
                    except Exception as ex:
                        print('Lo Sentimos, ha ocurrido un error inesperado!. \n')
                
                elif comando=='logout' and authenticator_conectado:
                    print('Te has desconectado de la sesion. \n')
                    token_list[0]=''
                    user=''
                    try:
                        timers[0].cancel()
                    except Exception as ex:
                        None
                    autentificado=False
                    
                elif comando=='add' and authenticator_conectado:
                    password_admin = getpass.getpass('Contraseña para Funciones Administrativas:')
                    user_a=input('Nombre de Usuario a Añadir:')
                    password_user = getpass.getpass('Contraseña del Usuario:')
                    
                    try:
                        servicioAuth.addUser(user_a,hashlib.sha256(password_user.encode()).hexdigest(),hashlib.sha256(password_admin.encode()).hexdigest())
                        print('Usuario Añadido Correctamente. \n')
                    except IceFlix.Unauthorized as ex:
                        print('Contraseña para Funciones Administrativas Incorrecta. \n')
                    except IceFlix.TemporaryUnavailable as ex:
                        print('Ha ocurrido algun error, pruebe de nuevo. \n')
                    except Exception as ex:
                        print('Lo Sentimos, ha ocurrido un error inesperado!. \n')

                elif comando=='remove' and authenticator_conectado:
                    password_admin = getpass.getpass('Contraseña para Funciones Administrativas:')
                    user_r=input('Nombre de Usuario a Eliminar:')
                    
                    if user==user_r:
                        print('Lo sentimos, No puede eliminar su propio usuario. \n')
                    else:
                        try:
                            servicioAuth.removeUser(user_r,hashlib.sha256(password_admin.encode()).hexdigest())
                            print('Usuario Eliminado Correctamente. \n')
                        except IceFlix.Unauthorized as ex:
                            print('Contraseña para Funciones Administrativas Incorrecta. \n')
                        except IceFlix.TemporaryUnavailable as ex:
                            print('Ha ocurrido algun error, pruebe de nuevo. \n')
                        except Exception as ex:
                            print('Lo Sentimos, ha ocurrido un error inesperado!. \n')


            if comando=='catalog':
                catalog_conectado=False
                try:
                    servicioCatalog = main.getCatalog()
                    catalog_conectado = True
                    
                    print("Servicio de Catalogo:\n"+
                "\t name -> Busqueda en el Catalogo por Nombre \n" +
                "\t tag -> Busqueda en el Catalogo por Tags \n" +
                "\t addtag -> Añadir tags a una busqueda previa \n" +
                "\t removetag -> Eliminar tags de una busqueda previa \n\n" +
                "Funciones Administrativas\n" +
                "\t rename -> Renombrar Titulos \n")
                    comando=input('Introduzca servicio deseado >')
                
                except IceFlix.TemporaryUnavailable as ex:
                    print('Lo sentimos, este servicio no esta disponible actualmente.')
    
                if comando =='name' and catalog_conectado:
                    print("\n¿Quiere hacer una busqueda que contenga el nombre, o que sea de forma exacta?")
                    exacta=input('Busqueda Exacta [s/n] >')
                    name=input('Nombre para hacer la busqueda >')
                    
                    try:
                        result = list()
                        if exacta=='s' or exacta=='S':
                            result = servicioCatalog.getTilesByName(name,True)
                        else:
                            result = servicioCatalog.getTilesByName(name,False)
                            
                        if len(result)!=0:
                            print('Resultados:')
                            busqueda_catalog=[]
                            for media_id in result:
                                media_result = servicioCatalog.getTile(media_id,'anonimo')
                                print('--'+media_result.info.name)
                                busqueda_catalog.append(media_result.mediaId)                  
                        else:
                            print('No hay resultados en su busqueda. \n')
                                
                    except Exception as ex:
                        print('Lo sentimos, ha ocurrido un error Inesperado!')
                    
                elif comando =='tag' and catalog_conectado:
                    if autentificado:
                        print("\n¿Quiere hacer una busqueda que contengan "+
                        "los tags exactos, o que contenga alguno de ellos?")
                        exacta=input('Busqueda de Tags Exacta [s/n] >')
                        string_tags =input('Introduzca los Tags [Tag1,Tag2,Tag3...] >')
                        list_tags = string_tags.split(',')
                    
                        try:
                            if exacta=='s' or exacta=='S':
                                result = servicioCatalog.getTilesByTags(list_tags,True,token_list[0])
                            else:
                                result = servicioCatalog.getTilesByTags(list_tags,False,token_list[0])
                        
                            if len(result)!=0:
                                print('Resultados:')
                                busqueda_catalog=[]
                                for media_id in result:
                                    media_result = servicioCatalog.getTile(media_id,'anonimo')
                                    print('--'+media_result.info.name)
                                    busqueda_catalog.append(media_result.mediaId)    
                            else:
                                print('No hay resultados en su busqueda. \n')
                        except Exception as ex:
                            print('Lo sentimos, ha ocurrido un error Inesperado!')
                    else:
                        print('Para hacer una busqueda por Tags debe primero autentificarse.')
                         	
                elif comando =='addtag' and catalog_conectado:
                    if autentificado:
                        if len(busqueda_catalog)!=0:
                            try:
                                id_titulo_seleccionado = self.seleccionar_busqueda(busqueda_catalog,servicioCatalog)
                                string_tags =input('Introduzca los Tags a añadir [Tag1,Tag2,Tag3...] >')
                                list_tags = string_tags.split(',')
                                servicioCatalog.addTags(id_titulo_seleccionado, list_tags,token_list[0])
                                print("Tags añadidos al Título Correctamente. \n")
                            except Exception as ex:
                                print('Lo sentimos, ha ocurrido un error Inesperado!') 
                        else:
                            print('Necesitas Primero hacer alguna busqueda para poder seleccionar Titulos. \n')
                    else:
                        print('Para añadir Tags a un título debe primero autentificarse.')  
                        
                                           
                elif comando =='removetag' and catalog_conectado:
                    if autentificado:
                        if len(busqueda_catalog)!=0:
                            try:
                                id_titulo_seleccionado = self.seleccionar_busqueda(busqueda_catalog,servicioCatalog)
                                string_tags =input('Introduzca los Tags a eliminar [Tag1,Tag2,Tag3...] >')
                                list_tags = string_tags.split(',')
                                servicioCatalog.removeTags(id_titulo_seleccionado, list_tags,token_list[0])
                                print("Tags eliminados del Título Correctamente. \n")
                            except Exception as ex:
                                print('Lo sentimos, ha ocurrido un error Inesperado!') 
                        else:
                            print('Necesitas Primero hacer alguna busqueda para poder seleccionar Titulos. \n')
                    else:
                        print('Para eliminar Tags de un título debe primero autentificarse.')  
                
                
                elif comando =='rename' and catalog_conectado:
                    if len(busqueda_catalog)!=0:
                        password_admin = getpass.getpass('Contraseña para Funciones Administrativas:')
                        try:
                            id_titulo_seleccionado = self.seleccionar_busqueda(busqueda_catalog,servicioCatalog)
                            if id_titulo_seleccionado!=None:
                                nuevo_nombre = input('Introduzca el nuevo nombre >')
                                servicioCatalog.renameTile(id_titulo_seleccionado,nuevo_nombre,hashlib.sha256(password_admin.encode()).hexdigest())
                                print('Titulo renombrado correctamente. \n')         
                        except IceFlix.Unauthorized as ex:
                            print('Contraseña para Funciones Administrativas Incorrecta. \n')
                        except Exception as ex:
                            print('Lo sentimos, ha ocurrido un error Inesperado!')               
                    else:
                        print('Necesitas Primero hacer alguna busqueda para poder seleccionar Titulos. \n')
        
        return 0

sys.exit(Client().main(sys.argv))
