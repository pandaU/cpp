#pragma execution_character_set("utf-8") 

#include "dataSourceManager.h"
#include "config.h"
#include "firebirdConnectManager.h"
#include "beFdeCommon/FdeException.h"
#include <sys/stat.h>
#include <string>
#include <fstream>

using namespace std;
using namespace beCore;
using namespace beFdeCore;

DataSourceManager * DataSourceManager::createInstace() 
{
	static DataSourceManager * instace = new DataSourceManager();
	return instace;
}

DataSourceManager::~DataSourceManager() {}

void DataSourceManager::initDataSourceManager() 
{

	beCore::AutoMutex L(m_mutex);
	IBPP::Database pFirebird = FirebirdConnectManager::createInstace()->getFirebirdConnect();
	//查询数据库全量物理数据集
	vector<t_physical_datasources> datasourceVec;
	MysqlTablePhysical::selectAll(pFirebird,datasourceVec);

	int pool_size = Config::createInstance()->getInt32("datasource", "min");	
	for (vector<t_physical_datasources>::iterator it = datasourceVec.begin(); it != datasourceVec.end(); it++)
	{
		vector<t_logical_datasources> logDataSourceVec;
		MysqlTableLogical::selectByPhyId(pFirebird, it->id, logDataSourceVec);
		for (vector<t_logical_datasources>::iterator itLog = logDataSourceVec.begin(); itLog != logDataSourceVec.end(); itLog++)
		{
			m_logphy_map.insert(make_pair(*itLog, *it));
		}

		/*按data source 初始化队列,方便后续操作*/
		if (it->type == PhysicalDataSourceType_FDE)
		{
			DataSourceCacheList dataSourceCacheList;
			m_cache_map.insert(make_pair(it->physicalDatasourceName, dataSourceCacheList));
		}
		else if (it->type == PhysicalDataSourceType_Terrain)
		{
			TerrainCacheList terrainCacheList;
			m_terraincache_map.insert(make_pair(it->physicalDatasourceName, terrainCacheList));
		}
		else if (it->type == PhysicalDataSourceType_3DTile)
		{
			TileCacheList tileCahceList;
			m_tilecache_map.insert(make_pair(it->physicalDatasourceName, tileCahceList));
		}

		for (int i = 0; i < pool_size; i++)
		{
			openDataSource(*it);
		}

	}
	FirebirdConnectManager::createInstace()->releaseFirebirdConnect(pFirebird);
}


void DataSourceManager::initNewDataSourceManager(const t_physical_datasources& data)
{
	beCore::AutoMutex L(m_mutex);
	IBPP::Database m_Firebird = FirebirdConnectManager::createInstace()->getFirebirdConnect();
	int pool_size = Config::createInstance()->getInt32("datasource", "min");

	vector<t_logical_datasources> logDataSourceVec;
	MysqlTableLogical::selectByPhyId(m_Firebird, data.id, logDataSourceVec);
	for (vector<t_logical_datasources>::iterator itLog = logDataSourceVec.begin(); itLog != logDataSourceVec.end(); itLog++)
	{
		m_logphy_map.insert(make_pair(*itLog, data));
	}

	/*按data source 初始化队列,方便后续操作*/
	if (data.type == PhysicalDataSourceType_FDE)
	{
		DataSourceCacheList dataSourceCacheList;
		m_cache_map.insert(make_pair(data.physicalDatasourceName, dataSourceCacheList));
	}
	else if (data.type == PhysicalDataSourceType_Terrain)
	{
		TerrainCacheList terrainCacheList;
		m_terraincache_map.insert(make_pair(data.physicalDatasourceName, terrainCacheList));
	}
	else if (data.type == PhysicalDataSourceType_3DTile)
	{
		TileCacheList tileCahceList;
		m_tilecache_map.insert(make_pair(data.physicalDatasourceName, tileCahceList));
	}
	for (int i = 0; i < pool_size; i++)
	{
		openDataSource(data);
	}
	FirebirdConnectManager::createInstace()->releaseFirebirdConnect(m_Firebird);
}

void DataSourceManager::openDataSource(const t_physical_datasources &info)
{
	//文件数据库db名为文件路径，需要特殊处理
	std::string file = info.db.substr(1, info.db.length() - 2);
	try
	{
		if (info.type == PhysicalDataSourceType_FDE)
		{
			beCore::Ptr<beFdeCore::DataSource> pDataSource;
			beCore::Ptr<ConnectionInfo> ci = ConnectionInfo::Create();
			ci->setConnectionType(info.finalDbType);
			if (info.finalDbType == CT_FIREBIRD2X)
			{
				ci->setServer(info.host);
				ci->setDatabase(file);
				ci->setPassword(info.passwd);
				ci->setUserName(info.user);
			}
			beFdeCore::DataSourceFactory &ins = DataSourceFactory::Instance();
			pDataSource = ins.openDataSource(ci);
			if (pDataSource != NULL)
			{
				map<string, DataSourceCacheList>::iterator it = m_cache_map.find(info.physicalDatasourceName);
				if (it != m_cache_map.end())
				{
					DataSourceCache dataSourceCache;
					dataSourceCache.pDataSource = pDataSource;
					it->second.push_front(dataSourceCache);
				}
			}
		}
		else if (info.type == PhysicalDataSourceType_Terrain)
		{
			beCore::Ptr<TEB::TEBReader> m_teb_reader = TEB::TEBReader::CreateInstance(0);
			m_teb_reader->open(file, "");

			if (m_teb_reader != NULL)
			{
				map<string, TerrainCacheList>::iterator it = m_terraincache_map.find(info.physicalDatasourceName);
				if (it != m_terraincache_map.end())
				{
					TerrainCache terrainCache;
					terrainCache.pTebReader = m_teb_reader;
					m_teb_reader->getTerrainInfo(&terrainCache.pInfo);
					it->second.push_front(terrainCache);
				}
			}
		}
		else if (info.type == PhysicalDataSourceType_3DTile)
		{
			ft::FileTibx* m_tile_reader = NULL;

			int errcode = ft::FileTibx::Create(&m_tile_reader, file, "");
			if (m_tile_reader != NULL)
			{
				map<string, TileCacheList>::iterator it = m_tilecache_map.find(info.physicalDatasourceName);
				if (it != m_tilecache_map.end())
				{
					TileCache tileCache;
					tileCache.pTileReader = m_tile_reader;
					it->second.push_front(tileCache);
				}
			}
		}
	}
	catch (beFdeCommon::FdeException& e)
	{
		for (std::map<t_logical_datasources, t_physical_datasources>::iterator it = m_logphy_map.begin(); it != m_logphy_map.end(); it++)
		{
			if (it->second.id == info.id)
			{
				m_logphy_map.erase(it);
				break;
			}
		}
		std::cout << e.getExceptionMessage() << std::endl;
	}

	
}


DataSourceCache DataSourceManager::getDataSourceCache(const std::string &index)
{
	beCore::AutoMutex L(m_mutex);
	DataSourceCache dataSourceCache;
	map<string, DataSourceCacheList >::iterator it = m_cache_map.find(index);
	if (it != m_cache_map.end())
	{
		if (it->second.size() > 0)
		{
			dataSourceCache = it->second.front();
			it->second.pop_front();
		}
		if (it->second.size()==0)
		{
			throw CException(ErrorUnknow, "DataSource connections use out");//数据源连接已用尽
		}
	}
	return dataSourceCache;
}

TerrainCache DataSourceManager::getTerrainCache(const std::string & index)
{
	beCore::AutoMutex L(m_mutex);
	TerrainCache terrainCache;
	map<string, TerrainCacheList >::iterator it = m_terraincache_map.find(index);
	if (it != m_terraincache_map.end())
	{
		if (it->second.size() > 0)
		{
			terrainCache = it->second.front();
			it->second.pop_front();
		}
		if (it->second.size() == 0)
		{
			throw CException(ErrorUnknow, "terrainCache connections use out");//数据源连接已用尽
		}
	}
	return terrainCache;
}

TileCache DataSourceManager::getTileCache(const std::string & index)
{
	beCore::AutoMutex L(m_mutex);
	TileCache tileCache;
	map<string, TileCacheList >::iterator it = m_tilecache_map.find(index);
	if (it != m_tilecache_map.end())
	{
		if (it->second.size() > 0)
		{
			tileCache = it->second.front();
			it->second.pop_front();
		}
		if (it->second.size() == 0)
		{
			throw CException(ErrorUnknow, "tileCache connections use out");//数据源连接已用尽
		}
	}
	return tileCache;
}

void DataSourceManager::releaseDataSourceCache(const std::string &index, DataSourceCache & dataSourceCache)
{
	if (dataSourceCache.pDataSource == NULL)
		return;
	beCore::AutoMutex L(m_mutex);
	map<string, DataSourceCacheList >::iterator it = m_cache_map.find(index);
	if (it != m_cache_map.end())
	{
		it->second.push_front(dataSourceCache);
	}
}

void DataSourceManager::releaseTerrainCache(const std::string &index, TerrainCache & terrainCache)
{
	if (terrainCache.pTebReader == NULL)
		return;
	beCore::AutoMutex L(m_mutex);
	map<string, TerrainCacheList >::iterator it = m_terraincache_map.find(index);
	if (it != m_terraincache_map.end())
	{
		it->second.push_front(terrainCache);
	}
}

void DataSourceManager::releaseTileCache(const std::string &index, TileCache & tileCache)
{
	if (tileCache.pTileReader == NULL)
		return;
	beCore::AutoMutex L(m_mutex);
	map<string, TileCacheList >::iterator it = m_tilecache_map.find(index);
	if (it != m_tilecache_map.end())
	{
		it->second.push_front(tileCache);
	}
}

beCore::Ptr<beFdeCore::FeatureDataSet> DataSourceManager::getFeatureDataSet(DataSourceCache & dataSourceCache, const string & featureDataSetName)
{
	beCore::Ptr<beFdeCore::FeatureDataSet> pDataSet;
	map<string, FeatureDataSetCache >::iterator it_dataSet = dataSourceCache.pFeatureDataSetMap.find(featureDataSetName);
	if (it_dataSet == dataSourceCache.pFeatureDataSetMap.end())
	{
		if (!dataSourceCache.pDataSource)
		{
			throw CException(ErrorSystem, "获取数据源指针失败");
		}
		pDataSet = dataSourceCache.pDataSource->openFeatureDataset(featureDataSetName);
		FeatureDataSetCache featureDataSetCache;
		featureDataSetCache.pFeatureDataSet = pDataSet;
		dataSourceCache.pFeatureDataSetMap.insert(make_pair(featureDataSetName, featureDataSetCache));
	}
	else 
	{
		pDataSet = it_dataSet->second.pFeatureDataSet;
	}
	return pDataSet;
}

beCore::Ptr<beFdeCore::ImageClass> DataSourceManager::getImageClass(DataSourceCache & dataSourceCache, const string & featureDataSetName)
{
	beCore::Ptr<beFdeCore::ImageClass> pImageClass;
	map<string, FeatureDataSetCache >::iterator it_dataSet = dataSourceCache.pFeatureDataSetMap.find(featureDataSetName);//featureDataSetName
	if (it_dataSet == dataSourceCache.pFeatureDataSetMap.end())
	{
		throw CException(ErrorSystem,"[FeatureDataSet]指针为空,操作失败.");
	}
	if (it_dataSet->second.pImageClass == NULL)
	{
		pImageClass  = it_dataSet->second.pFeatureDataSet->openImageClass();
		it_dataSet->second.pImageClass = pImageClass;
	}
	else 
	{
		pImageClass = it_dataSet->second.pImageClass;
	}
	return pImageClass;
}

beCore::Ptr<beFdeCore::ImageManager> DataSourceManager::getImageManager(DataSourceCache & dataSourceCache, const string & featureDataSetName)
{
	beCore::Ptr<beFdeCore::ImageManager> pImageManager;
	map<string, FeatureDataSetCache >::iterator it_dataSet = dataSourceCache.pFeatureDataSetMap.find(featureDataSetName);
	if (it_dataSet == dataSourceCache.pFeatureDataSetMap.end())
	{
		throw CException(ErrorSystem, "[FeatureDataSet]指针为空,操作失败.");
	}
	if (it_dataSet->second.pImageManager == NULL)
	{
		pImageManager = it_dataSet->second.pFeatureDataSet->getImageManager();
		it_dataSet->second.pImageManager = pImageManager;
	}
	else
	{
		pImageManager = it_dataSet->second.pImageManager;
	}
	return pImageManager;
}

beCore::Ptr<beFdeCore::ModelClass> DataSourceManager::getModelClass(DataSourceCache & dataSourceCache, const std::string & featureDataSetName)
{
	beCore::Ptr<beFdeCore::ModelClass> pModelClass;
	map<string, FeatureDataSetCache >::iterator it_dataSet = dataSourceCache.pFeatureDataSetMap.find(featureDataSetName);
	if (it_dataSet == dataSourceCache.pFeatureDataSetMap.end())
	{
		throw CException(ErrorSystem, "[FeatureDataSet]指针为空,操作失败.");
	}
	if (it_dataSet->second.pModelClass == NULL)
	{
		pModelClass = it_dataSet->second.pFeatureDataSet->openModelClass();
		it_dataSet->second.pModelClass = pModelClass;
	}
	else
	{
		pModelClass = it_dataSet->second.pModelClass;
	}
	return pModelClass;
}

beCore::Ptr<beFdeCore::FeatureClass> DataSourceManager::getFeatureClass(DataSourceCache & dataSourceCache, const std::string & featureDataSetName, const std::string & featureclassName)
{
	beCore::Ptr<beFdeCore::FeatureClass> pFeatureClass;
	map<string, FeatureDataSetCache >::iterator it_dataSet = dataSourceCache.pFeatureDataSetMap.find(featureDataSetName);
	if (it_dataSet == dataSourceCache.pFeatureDataSetMap.end())
	{
		throw CException(ErrorSystem, "[FeatureDataSet]指针为空,操作失败.");
	}
	map<string, beCore::Ptr<beFdeCore::FeatureClass>>::iterator it_featureClass = it_dataSet->second.pFeatureClassMap.find(featureclassName);
	if (it_featureClass == it_dataSet->second.pFeatureClassMap.end())
	{
		pFeatureClass = it_dataSet->second.pFeatureDataSet->openFeatureClass(featureclassName);
		it_dataSet->second.pFeatureClassMap.insert(make_pair(featureclassName, pFeatureClass));
	}
	else
	{
		pFeatureClass = it_featureClass->second;
	}
	return pFeatureClass;
}

beCore::Ptr<beFdeCore::ObjectClass> DataSourceManager::getObjectClass(DataSourceCache & dataSourceCache, const std::string & featureDataSetName, const std::string & objectclassName)
{
	beCore::Ptr<beFdeCore::ObjectClass> pObjectClass;
	map<string, FeatureDataSetCache >::iterator it_dataSet = dataSourceCache.pFeatureDataSetMap.find(featureDataSetName);
	if (it_dataSet == dataSourceCache.pFeatureDataSetMap.end())
	{
		throw CException(ErrorSystem, "[FeatureDataSet]指针为空,操作失败.");
	}
	map<string, beCore::Ptr<beFdeCore::ObjectClass>>::iterator it_ObjectClass = it_dataSet->second.pObjectClassMap.find(objectclassName);
	if (it_ObjectClass == it_dataSet->second.pObjectClassMap.end())
	{
		pObjectClass = it_dataSet->second.pFeatureDataSet->openObjectClass(objectclassName);
		it_dataSet->second.pObjectClassMap.insert(make_pair(objectclassName, pObjectClass));
	}
	else
	{
		pObjectClass = it_ObjectClass->second;
	}
	return pObjectClass;
}

beCore::Ptr<beFdeCore::Table> DataSourceManager::getTable(DataSourceCache & dataSourceCache, const string & tableName)
{
	beCore::Ptr<beFdeCore::Table> pTable;
	std::map<string, beCore::Ptr<beFdeCore::Table> >::iterator it_table = dataSourceCache.pTableMap.find(tableName);
	if (it_table == dataSourceCache.pTableMap.end())
	{
		if (!dataSourceCache.pDataSource)
		{
			throw CException(ErrorSystem, "获取数据源指针失败");
		}
		pTable = dataSourceCache.pDataSource->openTable(tableName);
		dataSourceCache.pTableMap.insert(make_pair(tableName, pTable));
	}
	else
	{
		pTable = it_table->second;
	}
	return pTable;

}