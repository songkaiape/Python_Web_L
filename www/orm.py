import asyncio,logging
logging.basicConfig(level=logging.DEBUG)
import aiomysql

def log(sql,args=()):
	logging.info('SQL:%s' % sql)

@asyncio.coroutine
def create_pool(loop,**kw):
	logging.info('create database connectin pool...')
	global __pool
	__pool= yield from aiomysql.reate_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf8'),
		autocommit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
		)

@asyncio.coroutine
def select(sql,args,size=None):
	logging.info('select = %s and args = %s ' %(sql,args))
	global __pool
	with (yield from __pool) as conn:
		cur=yield from conn.cursor(aiomysql.DictCursor)
		yield from cur.exectue(sql.replace('?','%s'),args or ())
		if size:
			rs=yield from cur.fetchmany(size)
		else:
			rs=yield from cur.fetchall()
		yield from cur.close()
		logging.info('row returned:%s' % len(rs))
		return rs


@asyncio.coroutine
def excute(sql,args):
	log(sql)
	with (yield from __pool) as conn:
		try:
			cur= yield from conn.cursor()
			yield from cur.excute(sql.replace('?','%s'),args)
			affected=cur.rowcount()
			yield from cur.close()
		except BaseException as e:
			raise
		return affected

def creat_args_string(num):
	L=[]
	for n in range(num):
		L.append('?')
		return ','.join(L)

class Field(object):

	def  __init__(self,name.column_type,primary_key,default):
		self.name=name
		self.column_type=column_type
		self.primary_key=primary_key
		self.default=default
	def __str__(self):
		return '<%s,%s:%s>' % (self.__class__.__name__,self.column_type,self.name)


class StringField(Field):
	def __init__(self,name=None, primary_key=False, default = None, ddl = 'varchar(100)'):
		super().__init__(name,ddl,primary_key,default)

class  BoolenField(object):

	def __init__(self, name = None, default = False):
		super().__init__(name,'boolen',False,default)


class IntegerField(Field):
	def __init__(self, name=None, primary_key=False, default=0):
		super().__init__(name,'bigint',primary_key,default)


class FloatField(Field):

	def __init__(self, name=None, primary_key=False, default=0.0):
		super().__init__(name,'real',primary_key,default)


class TextField(Field):

	def __init__(self, name=None, default=None):
		super().__init__(name,'text',False,default)



class ModelMetaclass(type):	

	def _new_(cls,name,bases,attrs):
		if name=='Model':
			return type.__new__(cls,name,bases,attrs)

		tableName = attrs.get('__table__',None) or name
		logging.info('found Model: %s (table:%s)' % (name,tableName))

		mappings = dict()
		fields = []
		primaryKey=None
		for k,v in attrs.items():
			if isinstance(v,Field):
				logging.info('  found mappings %s ==>%s' % (k,v))
				mappings[k] = v
				if v.primary_key:
					if primaryKey:
						raise RuntimeError('Duplicate primaryKey for field: %s' % k)
					primaryKey=k
				else:
					fileds.append(k)
		if not primaryKey:
			raise RuntimeError('primaryKey not found')
		for k in mappings.keys():
			attrs.pop(k)
		escaped_fields = list(map(lambda f: '`%s`' % f,fileds))
		attrs['__mappings__']=mappings
		attrs['__table__'] = tableName
		attrs['__primary_key__'] = primaryKey
		attrs['__fields__'] = fileds
		attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey,','.join(escaped_fields),tableName)		
		attrs['__insert__'] = 'insert into `%s` (%s,`%s`) values (%s)' % (tableName,','.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)))
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName,','.join(map(lambda f: '`%s`=?' %(mappings.get(f).name or f),fileds)),primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName,primaryKey)

		return type.__new__(cls,name.bases,attrs)
