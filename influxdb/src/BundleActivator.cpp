//
// BundleActivator.h
//
// Copyright (c) 2021, Applied Informatics Software Engineering GmbH.
// All rights reserved.
//


#include "MyDevices/ReflectorService.h"
#include "MyDevices/MetaPropertyService.h"
#include "Poco/OSP/BundleActivator.h"
#include "Poco/OSP/BundleContext.h"
#include "Poco/OSP/ServiceFinder.h"
#include "Poco/OSP/PreferencesService.h"
#include "Poco/Net/HTTPClientSession.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/NetException.h"
#include "Poco/URI.h"
#include "Poco/NotificationQueue.h"
#include "Poco/Format.h"
#include "Poco/NumberParser.h"
#include "Poco/StringTokenizer.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"
#include "Poco/SharedPtr.h"
#include "Poco/Delegate.h"
#include "Poco/StreamCopier.h"
#include "Poco/NullStream.h"
#include "Poco/ClassLibrary.h"
#include <sstream>
#include <map>


using namespace std::string_literals;


namespace MyDevices {
namespace InfluxDB {


class InfluxNotification: public Poco::Notification
{
public:
	using Ptr = Poco::AutoPtr<InfluxNotification>;
	using ValueMap = std::map<std::string, Poco::Dynamic::Var>;

	std::string measurement;
	ValueMap tags;
	ValueMap fields;
	Poco::Timestamp timestamp;
};


class BundleActivator: public Poco::OSP::BundleActivator, public Poco::Runnable
{
public:
	BundleActivator() = default;

	void start(Poco::OSP::BundleContext::Ptr pContext);
	void stop(Poco::OSP::BundleContext::Ptr pContext);

protected:
	void run();
	void onDeviceOnline(const std::string& deviceId);
	void onDeviceOffline(const std::string& deviceId);
	void onDevicePropertiesUpdated(const MyDevices::ReflectorService::DevicePropertiesUpdate& update);
	void onDeviceMetricsUpdated(const MyDevices::ReflectorService::DeviceMetricsUpdate& update);
	Poco::Dynamic::Var convertProperty(const std::string& name, const std::string& value);
	void writeChunk(const std::vector<InfluxNotification::Ptr>& buffer);
	void writeMeasurement(std::ostream& ostr, InfluxNotification::Ptr pNf);
	static std::string quoteValue(const Poco::Dynamic::Var& value);
	static std::string escape(const std::string& str);

private:
	Poco::OSP::BundleContext::Ptr _pContext;
	Poco::OSP::PreferencesService::Ptr _pPrefsService;
	MyDevices::ReflectorService::Ptr _pReflectorService;
	MyDevices::MetaPropertyService::Ptr _pMetaPropertyService;
	Poco::NotificationQueue _nfQueue;
	Poco::URI _influxURI;
	std::string _influxBucket;
	std::string _influxOrg;
	std::string _influxToken;
	long _chunkTimeout = 100;
	std::size_t _chunkSize = 100;
	std::set<std::string> _properties;
	bool _allProperties = false;
	Poco::Thread _thread;
};


void BundleActivator::start(Poco::OSP::BundleContext::Ptr pContext)
{
	_pContext = pContext;

	_pPrefsService = Poco::OSP::ServiceFinder::find<Poco::OSP::PreferencesService>(pContext);
	_pReflectorService = Poco::OSP::ServiceFinder::find<MyDevices::ReflectorService>(pContext);
	_pMetaPropertyService = Poco::OSP::ServiceFinder::find<MyDevices::MetaPropertyService>(pContext);

	_influxURI    = _pPrefsService->configuration()->getString("influxdb.uri"s, "http://localhost:8086/api/v2/write"s);
	_influxBucket = _pPrefsService->configuration()->getString("influxdb.bucket"s, "default"s);
	_influxOrg    = _pPrefsService->configuration()->getString("influxdb.organization"s, ""s);
	_influxToken  = _pPrefsService->configuration()->getString("influxdb.token"s, ""s);
	_chunkTimeout = _pPrefsService->configuration()->getUInt("writer.chunkTimeout"s, 200U);
	_chunkSize    = _pPrefsService->configuration()->getUInt("writer.chunkSize"s, 100U);

	const std::string properties = _pPrefsService->configuration()->getString("writer.properties"s, "*"s);
	if (properties == "*")
	{
		_allProperties = true;
	}
	else
	{
		Poco::StringTokenizer tok(properties, ","s, Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
		_properties.insert(tok.begin(), tok.end());
	}

	_pReflectorService->deviceOnline += Poco::delegate(this, &BundleActivator::onDeviceOnline);
	_pReflectorService->deviceOffline += Poco::delegate(this, &BundleActivator::onDeviceOffline);
	_pReflectorService->devicePropertiesUpdated += Poco::delegate(this, &BundleActivator::onDevicePropertiesUpdated);
	_pReflectorService->deviceMetricsUpdated += Poco::delegate(this, &BundleActivator::onDeviceMetricsUpdated);

	_thread.start(*this);
}


void BundleActivator::stop(Poco::OSP::BundleContext::Ptr pContext)
{
	_pReflectorService->deviceOnline -= Poco::delegate(this, &BundleActivator::onDeviceOnline);
	_pReflectorService->deviceOffline -= Poco::delegate(this, &BundleActivator::onDeviceOffline);
	_pReflectorService->devicePropertiesUpdated -= Poco::delegate(this, &BundleActivator::onDevicePropertiesUpdated);
	_pReflectorService->deviceMetricsUpdated -= Poco::delegate(this, &BundleActivator::onDeviceMetricsUpdated);

	_nfQueue.enqueueNotification(new InfluxNotification); // post empty notification to stop thread
	_thread.join();

	_pReflectorService.reset();
	_pMetaPropertyService.reset();
	_pPrefsService.reset();

	_pContext.reset();
}


void BundleActivator::run()
{
	bool stop = false;
	std::vector<InfluxNotification::Ptr> buffer;
	buffer.reserve(_chunkSize);
	while (!stop)
	{
		Poco::Notification::Ptr pNf = _nfQueue.waitDequeueNotification(_chunkTimeout);
		if (pNf)
		{
			InfluxNotification::Ptr pInfluxNf = pNf.cast<InfluxNotification>();
			if (pInfluxNf)
			{
				if (pInfluxNf->measurement.empty())
				{
					stop = true;
					pNf.reset(); // force writeChunk()
				}
				else
				{
					buffer.push_back(pInfluxNf);
				}
			}
		}
		if ((!pNf && !buffer.empty()) || buffer.size() >= _chunkSize)
		{
			try
			{
				writeChunk(buffer);
				buffer.clear();
			}
			catch (Poco::Exception& exc)
			{
				_pContext->logger().error("Error writing measurements: %s", exc.displayText());
			}
		}
	}
}


void BundleActivator::onDeviceOnline(const std::string& deviceId)
{
	InfluxNotification::Ptr pNf = new InfluxNotification;
	pNf->measurement = "status"s;
	pNf->tags["device"s] = deviceId;
	pNf->fields["online"s] = true;
	_nfQueue.enqueueNotification(pNf);
}


void BundleActivator::onDeviceOffline(const std::string& deviceId)
{
	InfluxNotification::Ptr pNf = new InfluxNotification;
	pNf->measurement = "status"s;
	pNf->tags["device"s] = deviceId;
	pNf->fields["online"s] = false;
	_nfQueue.enqueueNotification(pNf);
}


void BundleActivator::onDevicePropertiesUpdated(const MyDevices::ReflectorService::DevicePropertiesUpdate& update)
{
	try
	{
		InfluxNotification::Ptr pNf = new InfluxNotification;
		pNf->measurement = "properties"s;
		pNf->tags["device"s] = update.id;
		for (const auto& p: update.properties)
		{
			if (p.first == "ts")
			{
				pNf->timestamp = Poco::NumberParser::parse64(p.second);
			}
			else if (_allProperties || _properties.find(p.first) != _properties.end())
			{
				pNf->fields[p.first] = convertProperty(p.first, p.second);
			}
		}
		if (!pNf->fields.empty())
		{
			_nfQueue.enqueueNotification(pNf);
		}
	}
	catch (Poco::Exception& exc)
	{
		_pContext->logger().error("Error processing properties: %s"s, exc.displayText());
	}
}


void BundleActivator::onDeviceMetricsUpdated(const MyDevices::ReflectorService::DeviceMetricsUpdate& update)
{
	try
	{
		InfluxNotification::Ptr pNf = new InfluxNotification;
		pNf->measurement = "usage"s;
		pNf->tags["device"s] = update.id;
		pNf->fields["bytesTx"s] = update.bytesTx;
		pNf->fields["bytesRx"s] = update.bytesRx;
		_nfQueue.enqueueNotification(pNf);
	}
	catch (Poco::Exception& exc)
	{
		_pContext->logger().error("Error processing metrics: %s"s, exc.displayText());
	}
}


Poco::Dynamic::Var BundleActivator::convertProperty(const std::string& name, const std::string& value)
{
	auto pMetaProperty = _pMetaPropertyService->findProperty(name);
	switch (pMetaProperty->type())
	{
	case MyDevices::MetaProperty::TYPE_STRING:
	case MyDevices::MetaProperty::TYPE_DATETIME:
	case MyDevices::MetaProperty::TYPE_LATLON:
		return Poco::Dynamic::Var(value);

	case MyDevices::MetaProperty::TYPE_INT16:
	case MyDevices::MetaProperty::TYPE_INT32:
		return Poco::NumberParser::parse(value);

	case MyDevices::MetaProperty::TYPE_INT64:
		return Poco::NumberParser::parse64(value);

	case MyDevices::MetaProperty::TYPE_UINT16:
	case MyDevices::MetaProperty::TYPE_UINT32:
		return Poco::NumberParser::parse64(value);

	case MyDevices::MetaProperty::TYPE_UINT64:
		return Poco::NumberParser::parseUnsigned64(value);

	case MyDevices::MetaProperty::TYPE_DOUBLE:
		return Poco::Dynamic::Var(Poco::NumberParser::parseFloat(value));

	case MyDevices::MetaProperty::TYPE_BOOL:
		return Poco::Dynamic::Var(Poco::NumberParser::parseBool(value));

	default:
		return Poco::Dynamic::Var();
	}
}


void BundleActivator::writeChunk(const std::vector<InfluxNotification::Ptr>& buffer)
{
	Poco::Net::HTTPClientSession cs(_influxURI.getHost(), _influxURI.getPort());
	std::ostringstream path;
	path << _influxURI.getPath() << "?";
	Poco::Net::HTMLForm params;
	params.set("org"s, _influxOrg);
	params.set("bucket"s, _influxBucket);
	params.set("precision"s, "us"s);
	params.write(path);
	Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, path.str(), Poco::Net::HTTPRequest::HTTP_1_1);
	request.set("Authorization"s, Poco::format("Token %s"s, _influxToken));
	request.setContentType("text/plain; charset=utf-8"s);
	request.setChunkedTransferEncoding(true);

	if (_pContext->logger().debug())
	{
		std::ostringstream ostr;
		request.write(ostr);
		_pContext->logger().debug(ostr.str());
	}

	std::ostream& requestStream = cs.sendRequest(request);
	for (const auto& pNf: buffer)
	{
		writeMeasurement(requestStream, pNf);
	}
	Poco::Net::HTTPResponse response;
	std::istream& responseStream = cs.receiveResponse(response);
	std::string responseContent;
	Poco::StreamCopier::copyToString(responseStream, responseContent);
	if (response.getStatus() >= Poco::Net::HTTPResponse::HTTP_BAD_REQUEST)
	{
		_pContext->logger().error("Error writing chunk: %d %s: %s"s,
			static_cast<int>(response.getStatus()),
			response.getReason(),
			responseContent);
		throw Poco::Net::HTTPException(response.getReason(), static_cast<int>(response.getStatus()));
	}
}


void BundleActivator::writeMeasurement(std::ostream& ostr, InfluxNotification::Ptr pNf)
{
	ostr << pNf->measurement;
	for (const auto& t: pNf->tags)
	{
		ostr << "," << t.first << "=" << quoteValue(t.second);
	}
	ostr << " ";
	bool firstField = true;
	for (const auto& t: pNf->fields)
	{
		if (!firstField) ostr << ",";
		firstField = false;
		ostr << t.first << "=" << quoteValue(t.second);
	}
	ostr << " " << pNf->timestamp.epochMicroseconds() << "\n";
}


std::string BundleActivator::quoteValue(const Poco::Dynamic::Var& value)
{
	if (value.isBoolean())
	{
		return value.toString();
	}
	else if (value.isNumeric())
	{
		if (value.isInteger())
		{
			if (value.isSigned())
			{
				return value.toString() + "i";
			}
			else
			{
				return value.toString() + "u";
			}
		}
		else
		{
			return value.toString();
		}
	}
	else
	{
		std::string result("\"");
		result += escape(value.toString());
		result += "\"";
		return result;
	}
}


std::string BundleActivator::escape(const std::string& str)
{
	std::string result;
	for (const auto c: str)
	{
		if (c == '"')
		{
			result += "\\\"";
		}
		else
		{
			result += c;
		}
	}
	return result;
}


} } // namespace MyDevices::InfluxDB


POCO_BEGIN_MANIFEST(Poco::OSP::BundleActivator)
	POCO_EXPORT_CLASS(MyDevices::InfluxDB::BundleActivator)
POCO_END_MANIFEST
