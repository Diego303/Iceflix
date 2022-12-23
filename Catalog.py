#!/usr/bin/python3 -u
# -*- coding: utf-8 -*-

import sys
import uuid
from threading import Timer
import random
import Ice
import IceStorm
import sqlite3

try:
    Ice.loadSlice('iceflix.ice')
    import IceFlix
except ImportError:
    Ice.loadSlice(os.path.join(os.path.dirname(__file__), "iceflix.ice"))
    import IceFlix

class MediaCatalog(IceFlix.MediaCatalog):
    def __init__ (self, srvId, diccionario, stream_dic, catalog_updates):
        self.diccionario=diccionario
        self.primera_vez=True
        self.stream_dic=stream_dic
        self.catalog_updates = catalog_updates
        self.srvId = srvId
    
    def get_authenticator(self, current=None):
        """Metodo auxiliar para sacar un autenticador aleatorio para comprobar los token"""
        try:
            temp = random.choice(list(self.diccionario["Authenticator"]))
            self.diccionario["Authenticator"][temp].ice_ping()
            return self.diccionario["Authenticator"][temp]
        except Exception as ex:
            if len(self.diccionario["Authenticator"])==0:
                raise IceFlix.TemporaryUnavailable from ex
            else:
                self.diccionario["Authenticator"].pop(temp)
                return self.get_authenticator()

    def get_main(self, current=None):
        """Metodo auxiliar para sacar un main aleatorio para comprobar el admintoken"""
        try:
            temp = random.choice(list(self.diccionario["Main"]))
            self.diccionario["Main"][temp].ice_ping()
            return self.diccionario["Main"][temp]
        except Exception as ex:
            if len(self.diccionario["Main"])==0:
                raise IceFlix.TemporaryUnavailable from ex
            else:
                self.diccionario["Main"].pop(temp)
                return self.get_main()

    def get_media_row(self, id, current=None):
        db = sqlite3.connect("catalog.sqlite")
        cur = db.cursor()
        result = list(cur.execute("select * from media where id="+id))
        if len(result)!=0:
            result = result[0]
        db.close()
        return result

    def get_media_tags(self, mediaId, user_name, current=None):
        db = sqlite3.connect("catalog.sqlite")
        cur = db.cursor()
        resultSelect = list(cur.execute("SELECT tag FROM tags where username='"+user_name+"' and mediaid="+mediaId))
        result=list()
        for tmp in resultSelect:
            result.append(tmp[0])
        db.close()
        return result
       
    def get_media_name(self, mediaId, current=None):
        db = sqlite3.connect("catalog.sqlite")
        cur = db.cursor()
        result = list(cur.execute("SELECT name FROM media where id="+mediaId))
        if len(result)!=0:
            result = result[0]
        db.close()
        return result
    
    def get_ids_by_name(self, name, exact, current=None):

        db = sqlite3.connect("catalog.sqlite")
        cur = db.cursor()
        resultSelect = list()
        if exact:
            resultSelect = list(cur.execute("SELECT id FROM media where name='"+name+"'"))
        else:
            resultSelect = list(cur.execute("SELECT id FROM media where name like '%"+name+"%'"))
        result=list()
        for tmp in resultSelect:
            result.append(str(tmp[0]))
        db.close()
        return result
    
    def get_ids_by_tags(self, tags, exact, current=None):
        db = sqlite3.connect("catalog.sqlite")
        cur = db.cursor()
        
        str_list = ''
        tupla_tags = tuple(tags)
        str_list = str(tupla_tags)
        if len(tags)==1:
            str_list = "('"+tupla_tags[0]+"')"   
        resultSelect = list()
        if exact:
            resultSelect = list(cur.execute("select DISTINCT t.mediaid from tags as t where t.tag in "+str_list+" and (SELECT Count(a.tag) from tags as a where a.mediaid=t.mediaid)="+str(len(tupla_tags))))
        
        else:
            resultSelect = list(cur.execute("select DISTINCT mediaid from tags where tag in "+str_list))
              
        result=list()
        for tmp in resultSelect:
            result.append(str(tmp[0]))
        db.close()
        return result

    
    def getTile(self, mediaId, userToken, current=None):
        if userToken=='anonimo':
            media_row = self.get_media_row(mediaId)
            if len(media_row)==0:
                raise IceFlix.WrongMediaId
            else:
                try:
                    media = IceFlix.Media()
                    media.mediaId = mediaId    
                    media_info = IceFlix.MediaInfo()
                    result_media_name = self.get_media_name(mediaId)
                    media_info.name=result_media_name[0]
                    media.info = media_info

                    return media
                except Exception as ex:
                    print(ex)
                    raise IceFlix.TemporaryUnavailable from ex
        else:
            if (self.get_authenticator().isAuthorized(userToken)):
                media_row = self.get_media_row(mediaId)
                if len(media_row)==0:
                    raise IceFlix.WrongMediaId
                else:
                    try:
                        media = IceFlix.Media()
                        media.provider=None 
                        media.mediaId = mediaId
                    
                        media_info = IceFlix.MediaInfo()
                        user_name = self.get_authenticator().whois(userToken)
                    
                        result_media_name = self.get_media_name(mediaId)
                        media_info.name=result_media_name[0]
                    
                        media_info.tags = self.get_media_tags(mediaId, user_name)
                        media.info = media_info
                    
                        return media
                    except Exception as ex:
                        print(ex)
                        raise IceFlix.TemporaryUnavailable from ex
            else:
                raise IceFlix.Unauthorized

    def getTilesByName(self, name, exact, current=None):
        ids_name = self.get_ids_by_name(name,exact)
        return ids_name

    def getTilesByTags(self, tags, includeAllTags, userToken, current=None):
        if (self.get_authenticator().isAuthorized(userToken)):
            ids_tags = self.get_ids_by_tags(tags,includeAllTags)
            return ids_tags
        else:
            raise IceFlix.Unauthorized

    def addTags(self, mediaId, tags, userToken, current=None):
        if (self.get_authenticator().isAuthorized(userToken)):
            media_row = self.get_media_row(mediaId)
            if len(media_row)==0:
                raise IceFlix.WrongMediaId
            else:
                user_name = self.get_authenticator().whois(userToken)
                self.catalog_updates.addTags(mediaId, tags, user_name, self.srvId)
        else:
            raise IceFlix.Unauthorized

    def removeTags(self, mediaId, tags, userToken, current=None):
        if (self.get_authenticator().isAuthorized(userToken)):
            media_row = self.get_media_row(mediaId)
            if len(media_row)==0:
                raise IceFlix.WrongMediaId
            else:
                user_name = self.get_authenticator().whois(userToken)
                self.catalog_updates.removeTags(mediaId, tags, user_name, self.srvId)
        else:
            raise IceFlix.Unauthorized

    def renameTile(self, mediaId, name, adminToken, current=None):
        if (self.get_main().isAdmin(adminToken)):
            media_row = self.get_media_row(mediaId)
            if len(media_row)==0:
                raise IceFlix.WrongMediaId
            else:
                self.catalog_updates.renameTile(mediaId, name, self.srvId)
        else:
            raise IceFlix.Unauthorized

    def updateDB(self, catalogDatabase, srvId, current=None):
        if self.primera_vez:
            db = sqlite3.connect("catalog.sqlite")
            cur = db.cursor()
            cur.execute("delete from media")
            cur.execute("delete from tags")
            for media in catalogDatabase:
                cur.execute(f"insert into media (id, name) values ({media.mediaId}, '{media.name}')")
                for user in media.tagsPerUser.keys():
                    for tag in media.tagsPerUser[user]:
                        cur.execute(f"insert into tags (mediaid, username, tag) values ({media.mediaId}, '{user}', '{tag}')")

            cur.execute("update sqlite_sequence set seq=(select max(id) from media) where name='media'")
            db.commit()
            db.close()
            self.primera_vez=False

class CatalogUpdates(IceFlix.CatalogUpdates):
    def __init__ (self, diccionario):
        self.diccionario=diccionario

    def renameTile(self, mediaId, name, srvId, current=None):
        try:
            srv_catalog = self.diccionario["Catalog"][srvId]
            srv_catalog.ice_ping()
            
            db = sqlite3.connect("catalog.sqlite")
            cur = db.cursor()
            
            cur.execute("update media set name='"+name+"' where id="+mediaId)
            db.commit()
            db.close()
            
        except Exception as ex:
            None

    def addTags(self, mediaId, tags, user, srvId, current=None):
        try:
            srv_catalog = self.diccionario["Catalog"][srvId]
            srv_catalog.ice_ping()
            
            db = sqlite3.connect("catalog.sqlite")
            cur = db.cursor()
            
            for tag in tags:
                cur.execute("insert into tags Values ("+mediaId+",'"+user+"','"+tag+"')")
                db.commit()
            db.close()
            
        except Exception as ex:
            None

    def removeTags(self, mediaId, tags, user, srvId, current=None):
        try:
            srv_catalog = self.diccionario["Catalog"][srvId]
            srv_catalog.ice_ping()
            
            db = sqlite3.connect("catalog.sqlite")
            cur = db.cursor()
            
            for tag in tags:
                cur.execute("delete from tags where mediaid="+mediaId+" and username='"+user+"' and tag='"+tag+"'")
                db.commit()
            db.close()
            
        except Exception as ex:
            print(ex)
            None

class Revocations(IceFlix.Revocations):
    def __init__ (self, diccionario):
        self.diccionario=diccionario

    def revokeToken(self, userToken, srvId, current=None):
        """Incluido para cumplir con el interfaz, no se llama nunca aqui, usamos authenticator para comprobar cada vez"""
        None

    def revokeUser(self, user, srvId, current=None):
        db = sqlite3.connect("catalog.sqlite")
        cur = db.cursor()
        cur.execute("delete from tags where username='"+user+"'")
        db.commit()
        db.close()

class StreamAnnouncements(IceFlix.StreamAnnouncements):
    def __init__ (self, stream_dic):
        self.stream_dic=stream_dic

    def media_exists(self, mediaId):
        db = sqlite3.connect("catalog.sqlite")
        cur = db.cursor()
        if len(list(cur.execute(f"select id from media where id={mediaId}")))==0:
            return False
        else:
            return True

    def insert_media(self, mediaId, initialName):
        db = sqlite3.connect("catalog.sqlite")
        cur = db.cursor()
        cur.execute(f"insert into media (id, name) values ({mediaId},'{initialName}')")
        db.commit()
        db.close()

    def newMedia(self, mediaId, initialName, srvId, current=None):
        if not self.media_exists(mediaId):
            self.insert_media(mediaId, initialName)
        stream_dic[mediaId]=srvId
        None

    def removedMedia(self, mediaId, srvId, current=None):
        stream_dic[mediaId].pop()
        None

class ServiceAnnouncements(IceFlix.ServiceAnnouncements):

    def __init__(self, diccionario):
        self.diccionario=diccionario
    
    @property
    def known_services(self):
        """Get serviceIds for all services"""
        return list(self.diccionario["Authenticator"].keys()) + list(self.diccionario["Catalog"].keys()) + list(self.diccionario["Main"].keys())

    def get_media_list(self):
        db=sqlite3.connect("catalog.sqlite")
        cur=db.cursor()
        res=list(cur.execute("select * from media"))
        db.close()
        return res

    def get_catalog_database(self, current=None):
        catalog_database = []
        for media in self.get_media_list():
            tmp=IceFlix.MediaDB()
            tmp.name=media[1]
            tmp.mediaId=str(media[0])
            tmp.tagsPerUser=self.get_tags_user(media[0])
            catalog_database.append(tmp)
        return catalog_database

    def get_tags_user(self, mediaId):
        db=sqlite3.connect("catalog.sqlite")
        cur=db.cursor()
        res=list(cur.execute("select username, group_concat(tag) from tags where mediaid="+str(mediaId)+" group by username"))
        dic={}
        for t in res:
            dic[t[0]]=t[1].split(',')
        db.close()
        return dic

    def newService(self, service, srvId, current=None):
        """Check service type and add it."""
        if srvId in self.known_services:
            return
        if service.ice_isA('::IceFlix::Authenticator'):
            print(f'New Authenticator service: {srvId}')
            self.diccionario["Authenticator"][srvId] = IceFlix.AuthenticatorPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::Main'):
            print(f'New Main service: {srvId}')
            self.diccionario["Main"][srvId] = IceFlix.MainPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::StreamProvider'):
            print(f'New Stream service: {srvId}')
            service.reannounceMedia(0)
            self.diccionario["StreamProvider"][srvId] = IceFlix.MainPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::MediaCatalog'):
            """Si es un Catalog nuevo tenemos que actualizar la BD"""
            print(f'New MediaCatalog service: {srvId}')
            self.diccionario["Catalog"][srvId] = IceFlix.MediaCatalogPrx.uncheckedCast(service)
            new_catalog = IceFlix.MediaCatalogPrx.uncheckedCast(service)
            new_catalog.updateDB(self.get_catalog_database(),srvId)

    def announce(self, service, srvId, current=None):
        """Check service type and add it."""
        if srvId in self.known_services:
            return
        if service.ice_isA('::IceFlix::Authenticator'):
            print(f'Announce Authenticator service: {srvId}')
            self.diccionario["Authenticator"][srvId] = IceFlix.AuthenticatorPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::MediaCatalog'):
            print(f'Announce MediaCatalog service: {srvId}')
            self.diccionario["Catalog"][srvId] = IceFlix.MediaCatalogPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::Main'):
            self.diccionario["Main"][srvId] = IceFlix.MainPrx.uncheckedCast(service)
            print(f"Announce Main service: {srvId}")

class Catalog(Ice.Application):
    def get_topic_manager(self):
        key = 'IceStorm.TopicManager.Proxy'
        proxy = self.communicator().propertyToProxy(key)
        if proxy is None:
            print("property '{}' not set".format(key))
            return None

        print("Using IceStorm in: '%s'" % key)
        return IceStorm.TopicManagerPrx.checkedCast(proxy)

    def anunciar(self, announcements, ms, srvId, timer, diccionario, current=None):
        announcements.announce(ms, srvId)
        
        auths_invalidos=[]
        for obj in diccionario["Authenticator"]:
            try:
                diccionario["Authenticator"][obj].ice_ping()
            except Exception as ex:
                auths_invalidos.append(obj)

        for obj in auths_invalidos:
            diccionario["Authenticator"].pop(obj)

        catalog_invalidos=[]
        for obj in diccionario["Catalog"]:
            try:
                diccionario["Catalog"][obj].ice_ping()
            except Exception as ex:
                catalog_invalidos.append(obj)
        for obj in catalog_invalidos:
            diccionario["Catalog"].pop(obj)

        main_invalidos=[]
        for obj in diccionario["Main"]:
            try:
                diccionario["Main"][obj].ice_ping()
            except Exception as ex:
                main_invalidos.append(obj)
        for obj in main_invalidos:
            diccionario["Main"].pop(obj)
            
        timer = Timer((10.00+random.uniform(-2.00,2.00)),self.anunciar,(announcements, ms,srvId,timer, diccionario))
        timer.start()

    def run(self, argv):
        topic_mgr = self.get_topic_manager()
        if not topic_mgr:
            print("Invalid proxy")
            return 2

        diccionario= {"Authenticator":{},"Catalog":{},"Main":{}, "StreamProvider":{}} 
        stream_dic = {}
        broker = self.communicator()

        
        servAnnouncements = ServiceAnnouncements(diccionario)
        adapter = broker.createObjectAdapter("CatalogAdapter")
        ms_announcements = adapter.addWithUUID(servAnnouncements)
        topic_name = "serviceannouncements"

        qos = {}
        try:
            topic = topic_mgr.retrieve(topic_name)
        except IceStorm.NoSuchTopic:
            topic = topic_mgr.create(topic_name)

        topic.subscribeAndGetPublisher(qos, ms_announcements)
        print("Waiting events... '{}'".format(ms_announcements))
        
        """Suscriptor CatalogUpdates"""
        catalog_updates = CatalogUpdates(diccionario)
        ms_catalog_updates = adapter.addWithUUID(catalog_updates)

        topic_name = "catalogupdates"

        qos = {}
        try:
            topic_cu = topic_mgr.retrieve(topic_name)
        except IceStorm.NoSuchTopic:
            topic_cu = topic_mgr.create(topic_name)

        topic_cu.subscribeAndGetPublisher(qos, ms_catalog_updates)
        
        """Suscriptor revocations"""
        revocations = Revocations(diccionario)
        ms_revocations = adapter.addWithUUID(revocations)

        topic_name = "revocations"

        qos = {}
        try:
            topic_r = topic_mgr.retrieve(topic_name)
        except IceStorm.NoSuchTopic:
            topic_r = topic_mgr.create(topic_name)

        topic_r.subscribeAndGetPublisher(qos, ms_revocations)
        
        """Suscriptor StreamAnnouncements"""
        stream_announcements = StreamAnnouncements(stream_dic)
        ms_stream_announcements = adapter.addWithUUID(stream_announcements)

        topic_name = "streamannouncements"

        qos = {}
        try:
            topic_sa = topic_mgr.retrieve(topic_name)
        except IceStorm.NoSuchTopic:
            topic_sa = topic_mgr.create(topic_name)

        topic_sa.subscribeAndGetPublisher(qos, ms_stream_announcements)
        
        """ Publicador Anunciamientos """
        publisher = topic.getPublisher()
        announcements = IceFlix.ServiceAnnouncementsPrx.uncheckedCast(publisher)
        
        """ Publicador Catalog Updates """
        publisher = topic_cu.getPublisher()
        announcements_cu = IceFlix.CatalogUpdatesPrx.uncheckedCast(publisher)
        
        identificador = uuid.uuid4()
        
        servCatalog = MediaCatalog(str(identificador), diccionario,stream_dic, announcements_cu)
        ms_catalog = adapter.addWithUUID(servCatalog)#micro servicio de main
        adapter.activate()

        print("---Servicio catalog iniciado")
        diccionario["Catalog"][str(identificador)]=ms_catalog
        announcements.newService(ms_catalog, str(identificador))
        timer = None
        timer = Timer(3.00,self.anunciar,(announcements, ms_catalog, str(identificador),timer, diccionario))
        timer.start()
        
        self.shutdownOnInterrupt()
        broker.waitForShutdown()
        try:
            topic.unsubscribe(subscriber)
        except:
            print("\nSaliendo del servicio catalog")
        return 0

catalog = Catalog()
sys.exit(catalog.main(sys.argv))
