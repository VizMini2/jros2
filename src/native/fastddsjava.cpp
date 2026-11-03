#include "fastddsjava.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <unordered_map>

#include <fastdds/dds/core/StackAllocatedSequence.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipantListener.hpp>
#include <fastdds/dds/domain/qos/DomainParticipantExtendedQos.hpp>
#include <fastdds/utils/IPLocator.hpp>
#include <fastdds/rtps/common/SampleIdentity.hpp>
#include <fastdds/rtps/common/WriteParams.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/DataWriterListener.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/TopicDataType.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <fastdds/rtps/common/InstanceHandle.hpp>
#include <fastdds/rtps/common/SerializedPayload.hpp>

uint8_t* fastddsjava_TopicDataWrapper::data_ptr()
{
   return data_vector.data();
}

class fastddsjava_TopicDataWrapperType : public eprosima::fastdds::dds::TopicDataType
{
public:
   fastddsjava_TopicDataWrapperType(std::string name, uint16_t encapsulation)
   {
      set_name(name.c_str());
      this->encapsulation = encapsulation;
      max_serialized_type_size = 1 + 4;
      is_compute_key_provided = false;
   }

   bool serialize(const void* const data_, eprosima::fastdds::rtps::SerializedPayload_t& payload,
                  eprosima::fastdds::dds::DataRepresentationId_t data_representation) override
   {
      if (eprosima::fastdds::dds::DataRepresentationId_t::XCDR_DATA_REPRESENTATION != data_representation)
         return false;

      auto* data = const_cast<fastddsjava_TopicDataWrapper*>(static_cast<const fastddsjava_TopicDataWrapper*>(data_));

      payload.encapsulation = this->encapsulation;
      uint32_t data_length = calculate_serialized_size(data, data_representation);
      payload.length = data_length;
      memcpy(payload.data, data->data_vector.data(), data_length);
      payload.max_size = data_length;
      return true;
   }

   bool deserialize(eprosima::fastdds::rtps::SerializedPayload_t& payload, void* data_) override
   {
      auto* data = static_cast<fastddsjava_TopicDataWrapper*>(data_);
      data->data_vector.assign(payload.data, payload.data + payload.length);
      return true;
   }

   uint32_t calculate_serialized_size(const void* const data_,
                                      eprosima::fastdds::dds::DataRepresentationId_t data_representation) override
   {
      if (eprosima::fastdds::dds::DataRepresentationId_t::XCDR_DATA_REPRESENTATION != data_representation)
         return 0;

      auto* data = const_cast<fastddsjava_TopicDataWrapper*>(static_cast<const fastddsjava_TopicDataWrapper*>(data_));
      return static_cast<uint32_t>(data->data_vector.size());
   }

   bool compute_key(eprosima::fastdds::rtps::SerializedPayload_t& payload, eprosima::fastdds::rtps::InstanceHandle_t& ihandle,
                    bool force_md5 = false) override
   {
      (void)payload;
      (void)ihandle;
      (void)force_md5;
      return true;
   }

   bool compute_key(const void* const data, eprosima::fastdds::rtps::InstanceHandle_t& ihandle, bool force_md5 = false) override
   {
      (void)data;
      (void)ihandle;
      (void)force_md5;
      return true;
   }

   void* create_data() override
   {
      auto* data = new fastddsjava_TopicDataWrapper();
      data->data_vector = std::vector<uint8_t>(1, 0);
      return reinterpret_cast<void*>(data);
   }

   void delete_data(void* data) override
   {
      delete reinterpret_cast<fastddsjava_TopicDataWrapper*>(data);
   }

   const std::string& get_name() const
   {
      return eprosima::fastdds::dds::TopicDataType::get_name();
   }

private:
   uint16_t encapsulation;
};

class fastddsjava_DataReaderListener : public eprosima::fastdds::dds::DataReaderListener
{
public:
   void set_on_data_available_callback(std::function<void()> callback)
   {
      on_data_callback = std::move(callback);
   }

   void set_on_subscription_callback(std::function<void()> callback)
   {
      on_subscription_callback = std::move(callback);
   }

   void on_data_available(eprosima::fastdds::dds::DataReader* reader) override
   {
      (void)reader;
      if (on_data_callback)
         on_data_callback();
   }

   void on_subscription_matched(eprosima::fastdds::dds::DataReader* reader,
                                const eprosima::fastdds::dds::SubscriptionMatchedStatus& info) override
   {
      (void)reader;
      (void)info;
      if (on_subscription_callback)
         on_subscription_callback();
   }

private:
   std::function<void()> on_data_callback;
   std::function<void()> on_subscription_callback;
};

class fastddsjava_DataWriterListener : public eprosima::fastdds::dds::DataWriterListener
{
public:
   void set_on_publication_callback(std::function<void()> callback)
   {
      on_publication_callback = std::move(callback);
   }

   void on_publication_matched(eprosima::fastdds::dds::DataWriter* writer,
                               const eprosima::fastdds::dds::PublicationMatchedStatus& info) override
   {
      (void)writer;
      (void)info;
      if (on_publication_callback)
         on_publication_callback();
   }

private:
   std::function<void()> on_publication_callback;
};

uint32_t fastddsjava_load_xml_profiles_string(const std::string& xml)
{
   auto factory = eprosima::fastdds::dds::DomainParticipantFactory::get_instance();
   return factory->load_XML_profiles_string(xml.c_str(), xml.length());
}

namespace
{
// Keeps the ROS graph (every endpoint and participant of the domain) from the builtin discovery
// callbacks, so it can be listed like `ros2 topic list` / `ros2 service list`. Discovery callbacks
// are delivered regardless of the StatusMask, so it is attached with StatusMask::none() and never
// steals data callbacks from the readers' own listeners.
class fastddsjava_GraphListener : public eprosima::fastdds::dds::DomainParticipantListener
{
public:
   void on_participant_discovery(eprosima::fastdds::dds::DomainParticipant*, eprosima::fastdds::rtps::ParticipantDiscoveryStatus reason,
                                 const eprosima::fastdds::rtps::ParticipantBuiltinTopicData& info, bool& should_be_ignored) override
   {
      should_be_ignored = false;
      using Status = eprosima::fastdds::rtps::ParticipantDiscoveryStatus;
      std::lock_guard<std::mutex> lock(mutex);
      if (reason == Status::REMOVED_PARTICIPANT || reason == Status::DROPPED_PARTICIPANT || reason == Status::IGNORED_PARTICIPANT)
      {
         participants.erase(info.guid.guidPrefix);
         addresses.erase(info.guid.guidPrefix);
         // Endpoints of a participant that went away are not always reported one by one.
         for (auto it = endpoints.begin(); it != endpoints.end();)
         {
            if (it->first.guidPrefix == info.guid.guidPrefix)
               it = endpoints.erase(it);
            else
               ++it;
         }
      }
      else
      {
         participants[info.guid.guidPrefix] = info.participant_name.to_string();
         std::vector<std::string> ips;
         for (const auto& locator : info.metatraffic_locators.unicast)
         {
            if (locator.kind != LOCATOR_KIND_UDPv4 || eprosima::fastdds::rtps::IPLocator::isLocal(locator))
               continue;
            std::string ip = eprosima::fastdds::rtps::IPLocator::toIPv4string(locator);
            if (std::find(ips.begin(), ips.end(), ip) == ips.end())
               ips.push_back(ip);
         }
         addresses[info.guid.guidPrefix] = ips;
      }
   }

   void on_data_reader_discovery(eprosima::fastdds::dds::DomainParticipant*, eprosima::fastdds::rtps::ReaderDiscoveryStatus reason,
                                 const eprosima::fastdds::dds::SubscriptionBuiltinTopicData& info, bool& should_be_ignored) override
   {
      should_be_ignored = false;
      using Status = eprosima::fastdds::rtps::ReaderDiscoveryStatus;
      update(info.guid, false, info.topic_name.to_string(), info.type_name.to_string(),
             reason == Status::REMOVED_READER || reason == Status::IGNORED_READER);
   }

   void on_data_writer_discovery(eprosima::fastdds::dds::DomainParticipant*, eprosima::fastdds::rtps::WriterDiscoveryStatus reason,
                                 const eprosima::fastdds::dds::PublicationBuiltinTopicData& info, bool& should_be_ignored) override
   {
      should_be_ignored = false;
      using Status = eprosima::fastdds::rtps::WriterDiscoveryStatus;
      update(info.guid, true, info.topic_name.to_string(), info.type_name.to_string(),
             reason == Status::REMOVED_WRITER || reason == Status::IGNORED_WRITER);
   }

   std::vector<fastddsjava_DiscoveredEndpoint> snapshot()
   {
      std::lock_guard<std::mutex> lock(mutex);
      std::vector<fastddsjava_DiscoveredEndpoint> out;
      out.reserve(endpoints.size());
      for (const auto& entry : endpoints)
         out.push_back(entry.second);
      return out;
   }

   std::vector<std::string> participant_addresses()
   {
      static const char* hex = "0123456789abcdef";
      std::lock_guard<std::mutex> lock(mutex);
      std::vector<std::string> out;
      for (const auto& entry : participants)
      {
         std::string line;
         for (uint8_t b : entry.first.value)
         {
            line += hex[b >> 4];
            line += hex[b & 0xF];
         }
         line += '\t';
         line += entry.second;
         line += '\t';
         auto it = addresses.find(entry.first);
         if (it != addresses.end())
         {
            for (size_t i = 0; i < it->second.size(); ++i)
            {
               if (i > 0)
                  line += ',';
               line += it->second[i];
            }
         }
         out.push_back(line);
      }
      return out;
   }

   std::vector<std::string> participant_names()
   {
      std::lock_guard<std::mutex> lock(mutex);
      std::vector<std::string> out;
      for (const auto& entry : participants)
         out.push_back(entry.second);
      return out;
   }

private:
   void update(const eprosima::fastdds::rtps::GUID_t& guid, bool is_writer, std::string topic, std::string type, bool removed)
   {
      std::lock_guard<std::mutex> lock(mutex);
      if (removed)
      {
         endpoints.erase(guid);
         return;
      }
      fastddsjava_DiscoveredEndpoint& endpoint = endpoints[guid];
      endpoint.is_writer = is_writer;
      endpoint.topic_name = std::move(topic);
      endpoint.type_name = std::move(type);
      memcpy(endpoint.guid, guid.guidPrefix.value, 12);
      memcpy(endpoint.guid + 12, guid.entityId.value, 4);
   }

   std::mutex mutex;
   std::map<eprosima::fastdds::rtps::GUID_t, fastddsjava_DiscoveredEndpoint> endpoints;
   std::map<eprosima::fastdds::rtps::GuidPrefix_t, std::string> participants;
   std::map<eprosima::fastdds::rtps::GuidPrefix_t, std::vector<std::string>> addresses;
};

std::mutex g_graph_mutex;
std::unordered_map<void*, fastddsjava_GraphListener*> g_graph_listeners;

fastddsjava_GraphListener* graph_listener(void* participant_)
{
   std::lock_guard<std::mutex> lock(g_graph_mutex);
   auto it = g_graph_listeners.find(participant_);
   return it == g_graph_listeners.end() ? nullptr : it->second;
}

// Identity of the last sample written by each writer (only writers that have written at least once).
std::mutex g_identity_mutex;
std::unordered_map<void*, eprosima::fastdds::rtps::SampleIdentity> g_last_identity;

void write_identity(const eprosima::fastdds::rtps::SampleIdentity& id, uint8_t* out)
{
   memcpy(out, id.writer_guid().guidPrefix.value, 12);
   memcpy(out + 12, id.writer_guid().entityId.value, 4);
   int32_t high = id.sequence_number().high;
   uint32_t low = id.sequence_number().low;
   for (int i = 0; i < 4; ++i)
   {
      out[16 + i] = static_cast<uint8_t>((static_cast<uint32_t>(high) >> (24 - 8 * i)) & 0xFF);
      out[20 + i] = static_cast<uint8_t>((low >> (24 - 8 * i)) & 0xFF);
   }
}
} // namespace

// Participant ids reached on each peer (ROS 2 creates one participant per process).
constexpr uint32_t PEER_PARTICIPANT_IDS = 32;
std::mutex g_peers_mutex;
std::vector<std::string> g_initial_peers;

std::vector<std::string> g_discovery_servers;

void fastddsjava_set_initial_peers(const std::vector<std::string>& peers)
{
   std::lock_guard<std::mutex> lock(g_peers_mutex);
   g_initial_peers = peers;
}

void fastddsjava_set_discovery_servers(const std::vector<std::string>& servers)
{
   std::lock_guard<std::mutex> lock(g_peers_mutex);
   g_discovery_servers = servers;
}

void* fastddsjava_create_participant(const std::string& profile_name)
{
   auto factory = eprosima::fastdds::dds::DomainParticipantFactory::get_instance();
   auto* listener = new fastddsjava_GraphListener();
   std::vector<std::string> peers;
   std::vector<std::string> servers;
   {
      std::lock_guard<std::mutex> lock(g_peers_mutex);
      peers = g_initial_peers;
      servers = g_discovery_servers;
   }
   void* participant = nullptr;
   eprosima::fastdds::dds::DomainParticipantExtendedQos qos;
   if ((!peers.empty() || !servers.empty()) &&
       factory->get_participant_extended_qos_from_profile(profile_name, qos) == eprosima::fastdds::dds::RETCODE_OK)
   {
      if (!servers.empty())
      {
         auto& discovery = qos.wire_protocol().builtin.discovery_config;
         discovery.discoveryProtocol = eprosima::fastdds::rtps::DiscoveryProtocol::SUPER_CLIENT;
         for (const std::string& server : servers)
         {
            // "ip" or "ip:port" (11811 by default, like ROS_DISCOVERY_SERVER).
            std::string address = server;
            uint32_t port = 11811;
            size_t colon = server.rfind(':');
            if (colon != std::string::npos)
            {
               address = server.substr(0, colon);
               port = static_cast<uint32_t>(std::strtoul(server.c_str() + colon + 1, nullptr, 10));
            }
            eprosima::fastdds::rtps::Locator_t locator;
            locator.kind = LOCATOR_KIND_UDPv4;
            if (port > 0 && port <= 65535 && eprosima::fastdds::rtps::IPLocator::setIPv4(locator, address))
            {
               locator.port = port;
               discovery.m_DiscoveryServers.push_back(locator);
            }
         }
      }

      if (!peers.empty())
      {
         // Port 0 would only reach participant ids 0..maxInitialPeersRange (4 by default): a robot easily
         // runs more processes than that, so the well-known unicast ports of many participant ids are listed.
         auto& list = qos.wire_protocol().builtin.initialPeersList;
         const auto& ports = qos.wire_protocol().port;
         const uint32_t domain = qos.domainId();
         if (list.empty())
         {
            // Setting initial peers replaces the default multicast one: keep it explicitly.
            eprosima::fastdds::rtps::Locator_t multicast;
            multicast.kind = LOCATOR_KIND_UDPv4;
            eprosima::fastdds::rtps::IPLocator::setIPv4(multicast, "239.255.0.1");
            multicast.port = 0;
            list.push_back(multicast);
         }
         for (const std::string& peer : peers)
         {
            eprosima::fastdds::rtps::Locator_t locator;
            locator.kind = LOCATOR_KIND_UDPv4;
            if (!eprosima::fastdds::rtps::IPLocator::setIPv4(locator, peer))
               continue;
            for (uint32_t participant_id = 0; participant_id < PEER_PARTICIPANT_IDS; ++participant_id)
            {
               uint32_t port = ports.portBase + ports.domainIDGain * domain + ports.offsetd1 + ports.participantIDGain * participant_id;
               if (port > 65535)
                  break;
               locator.port = port;
               list.push_back(locator);
            }
         }
      }
      participant = factory->create_participant(qos, listener, eprosima::fastdds::dds::StatusMask::none());
   }
   else
   {
      participant = factory->create_participant_with_profile(profile_name, listener, eprosima::fastdds::dds::StatusMask::none());
   }
   if (participant == nullptr)
   {
      delete listener;
      return nullptr;
   }
   std::lock_guard<std::mutex> lock(g_graph_mutex);
   g_graph_listeners[participant] = listener;
   return participant;
}

uint32_t fastddsjava_delete_participant(void* participant_)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   auto factory = eprosima::fastdds::dds::DomainParticipantFactory::get_instance();
   uint32_t ret = factory->delete_participant(participant);
   if (ret == eprosima::fastdds::dds::RETCODE_OK)
   {
      // The listener can only go once the participant (and its discovery threads) is gone.
      fastddsjava_GraphListener* listener = nullptr;
      {
         std::lock_guard<std::mutex> lock(g_graph_mutex);
         auto it = g_graph_listeners.find(participant_);
         if (it != g_graph_listeners.end())
         {
            listener = it->second;
            g_graph_listeners.erase(it);
         }
      }
      delete listener;
   }
   return ret;
}

std::vector<fastddsjava_DiscoveredEndpoint> fastddsjava_participant_get_discovered_endpoints(void* participant_)
{
   fastddsjava_GraphListener* listener = graph_listener(participant_);
   return listener != nullptr ? listener->snapshot() : std::vector<fastddsjava_DiscoveredEndpoint>();
}

std::vector<std::string> fastddsjava_participant_get_discovered_participant_addresses(void* participant_)
{
   fastddsjava_GraphListener* listener = graph_listener(participant_);
   return listener != nullptr ? listener->participant_addresses() : std::vector<std::string>();
}

std::vector<std::string> fastddsjava_participant_get_discovered_participant_names(void* participant_)
{
   fastddsjava_GraphListener* listener = graph_listener(participant_);
   return listener != nullptr ? listener->participant_names() : std::vector<std::string>();
}

bool fastddsjava_datawriter_get_last_sample_identity(void* writer_, uint8_t* identity_out)
{
   std::lock_guard<std::mutex> lock(g_identity_mutex);
   auto it = g_last_identity.find(writer_);
   if (it == g_last_identity.end())
      return false;
   write_identity(it->second, identity_out);
   return true;
}

void fastddsjava_sample_info_get_sample_identity(eprosima::fastdds::dds::SampleInfo* info, uint8_t* identity_out)
{
   write_identity(info->sample_identity, identity_out);
}

void fastddsjava_sample_info_get_related_sample_identity(eprosima::fastdds::dds::SampleInfo* info, uint8_t* identity_out)
{
   write_identity(info->related_sample_identity, identity_out);
}

void* fastddsjava_create_publisher(void* participant_, const std::string& profile_name)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   return participant->create_publisher_with_profile(profile_name);
}

uint32_t fastddsjava_delete_publisher(void* participant_, void* publisher_)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   auto* publisher = static_cast<eprosima::fastdds::dds::Publisher*>(publisher_);
   return participant->delete_publisher(publisher);
}

void* fastddsjava_create_subscriber(void* participant_, const std::string& profile_name)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   return participant->create_subscriber_with_profile(profile_name);
}

uint32_t fastddsjava_delete_subscriber(void* participant_, void* subscriber_)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   auto* subscriber = static_cast<eprosima::fastdds::dds::Subscriber*>(subscriber_);
   return participant->delete_subscriber(subscriber);
}

fastddsjava_TopicDataWrapperType* fastddsjava_create_topic_data_wrapper_type(const std::string& name, uint16_t encapsulation)
{
   return new fastddsjava_TopicDataWrapperType(name, encapsulation);
}

void* fastddsjava_create_typesupport(fastddsjava_TopicDataWrapperType* type)
{
   return new eprosima::fastdds::dds::TypeSupport(type);
}

uint32_t fastddsjava_register_type(void* participant_, void* type_support_)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   auto* type_support = static_cast<eprosima::fastdds::dds::TypeSupport*>(type_support_);
   return participant->register_type(*type_support);
}

uint32_t fastddsjava_unregister_type(void* participant_, const std::string& type_name)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   return participant->unregister_type(type_name);
}

void fastddsjava_delete_typesupport(void* type_support_)
{
   // TypeSupport is a std::shared_ptr subclass allocated with new in create_typesupport.
   auto* type_support = static_cast<eprosima::fastdds::dds::TypeSupport*>(type_support_);
   type_support->~TypeSupport();
   ::operator delete(type_support);
}

std::string fastddsjava_topic_data_wrapper_type_get_name(fastddsjava_TopicDataWrapperType* type)
{
   return type->get_name();
}

fastddsjava_TopicDataWrapper* fastddsjava_topic_data_wrapper_type_create_data(fastddsjava_TopicDataWrapperType* type)
{
   return static_cast<fastddsjava_TopicDataWrapper*>(type->create_data());
}

void fastddsjava_topic_data_wrapper_type_delete_data(fastddsjava_TopicDataWrapperType* type, fastddsjava_TopicDataWrapper* data)
{
   type->delete_data(data);
}

void* fastddsjava_create_topic(void* participant_, fastddsjava_TopicDataWrapperType* type, const std::string& topic_name,
                               const std::string& profile_name)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   return participant->create_topic_with_profile(topic_name, type->get_name(), profile_name);
}

uint32_t fastddsjava_delete_topic(void* participant_, void* topic_)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   auto* topic = static_cast<eprosima::fastdds::dds::Topic*>(topic_);
   return participant->delete_topic(topic);
}

void* fastddsjava_create_datawriter(void* publisher_, void* topic_, const std::string& profile_name)
{
   auto* publisher = static_cast<eprosima::fastdds::dds::Publisher*>(publisher_);
   auto* topic = static_cast<eprosima::fastdds::dds::Topic*>(topic_);
   return publisher->create_datawriter_with_profile(topic, profile_name);
}

uint32_t fastddsjava_delete_datawriter(void* publisher_, void* writer_)
{
   auto* publisher = static_cast<eprosima::fastdds::dds::Publisher*>(publisher_);
   auto* writer = static_cast<eprosima::fastdds::dds::DataWriter*>(writer_);
   {
      std::lock_guard<std::mutex> lock(g_identity_mutex);
      g_last_identity.erase(writer_);
   }
   return publisher->delete_datawriter(writer);
}

uint32_t fastddsjava_datawriter_write(void* writer_, fastddsjava_TopicDataWrapper* data)
{
   auto* writer = static_cast<eprosima::fastdds::dds::DataWriter*>(writer_);
   // WriteParams returns the identity Fast DDS gave the sample: a service client needs it to tell
   // which reply (related_sample_identity) answers which request.
   eprosima::fastdds::rtps::WriteParams params;
   uint32_t ret = writer->write(data, params);
   if (ret == eprosima::fastdds::dds::RETCODE_OK)
   {
      std::lock_guard<std::mutex> lock(g_identity_mutex);
      g_last_identity[writer_] = params.sample_identity();
   }
   return ret;
}

uint32_t fastddsjava_datawriter_set_listener(void* writer_, fastddsjava_DataWriterListener* listener)
{
   auto* writer = static_cast<eprosima::fastdds::dds::DataWriter*>(writer_);
   return writer->set_listener(listener);
}

uint32_t fastddsjava_datawriter_get_publication_matched_status(void* writer_, eprosima::fastdds::dds::PublicationMatchedStatus& status)
{
   auto* writer = static_cast<eprosima::fastdds::dds::DataWriter*>(writer_);
   return writer->get_publication_matched_status(status);
}

void* fastddsjava_create_datareader(void* subscriber_, void* topic_, fastddsjava_DataReaderListener* listener,
                                    const std::string& profile_name)
{
   auto* subscriber = static_cast<eprosima::fastdds::dds::Subscriber*>(subscriber_);
   auto* topic = static_cast<eprosima::fastdds::dds::Topic*>(topic_);
   return subscriber->create_datareader_with_profile(topic, profile_name, listener, eprosima::fastdds::dds::StatusMask::all());
}

uint32_t fastddsjava_delete_datareader(void* subscriber_, void* reader_)
{
   auto* subscriber = static_cast<eprosima::fastdds::dds::Subscriber*>(subscriber_);
   auto* reader = static_cast<eprosima::fastdds::dds::DataReader*>(reader_);
   return subscriber->delete_datareader(reader);
}

uint32_t fastddsjava_datareader_read_next_sample(void* reader_, void* data, eprosima::fastdds::dds::SampleInfo* info)
{
   auto* reader = static_cast<eprosima::fastdds::dds::DataReader*>(reader_);
   return reader->read_next_sample(data, info);
}

uint32_t fastddsjava_datareader_take_next_custom(void* reader_, void* data, eprosima::fastdds::dds::SampleInfo* info)
{
   auto* reader = static_cast<eprosima::fastdds::dds::DataReader*>(reader_);

   eprosima::fastdds::dds::StackAllocatedSequence<void*, 1> data_values;
   const_cast<void**>(data_values.buffer())[0] = data;
   eprosima::fastdds::dds::LoanableSequence<eprosima::fastdds::dds::SampleInfo> sample_infos(1);

   eprosima::fastdds::dds::ReturnCode_t ret = reader->take(data_values, sample_infos, 1, eprosima::fastdds::dds::ANY_SAMPLE_STATE,
                                                           eprosima::fastdds::dds::ANY_VIEW_STATE, eprosima::fastdds::dds::ANY_INSTANCE_STATE);

   if (eprosima::fastdds::dds::RETCODE_OK == ret)
   {
      *info = sample_infos[0];
   }

   return ret;
}

uint32_t fastddsjava_datareader_set_listener(void* reader_, fastddsjava_DataReaderListener* listener)
{
   auto* reader = static_cast<eprosima::fastdds::dds::DataReader*>(reader_);
   return reader->set_listener(listener);
}

uint32_t fastddsjava_datareader_get_unread_count(void* reader_)
{
   auto* reader = static_cast<eprosima::fastdds::dds::DataReader*>(reader_);
   return reader->get_unread_count();
}

uint32_t fastddsjava_datareader_get_subscription_matched_status(void* reader_,
                                                                eprosima::fastdds::dds::SubscriptionMatchedStatus& status)
{
   auto* reader = static_cast<eprosima::fastdds::dds::DataReader*>(reader_);
   return reader->get_subscription_matched_status(status);
}

void fastddsjava_get_participant_guid(void* participant_, uint8_t* guid_out)
{
   auto* participant = static_cast<eprosima::fastdds::dds::DomainParticipant*>(participant_);
   eprosima::fastdds::rtps::GUID_t guid = participant->guid();
   memcpy(guid_out, guid.guidPrefix.value, 12);
   memcpy(guid_out + 12, guid.entityId.value, 4);
}

void fastddsjava_get_writer_guid(void* writer_, uint8_t* guid_out)
{
   auto* writer = static_cast<eprosima::fastdds::dds::DataWriter*>(writer_);
   eprosima::fastdds::rtps::GUID_t guid = writer->guid();
   memcpy(guid_out, guid.guidPrefix.value, 12);
   memcpy(guid_out + 12, guid.entityId.value, 4);
}

void fastddsjava_get_reader_guid(void* reader_, uint8_t* guid_out)
{
   auto* reader = static_cast<eprosima::fastdds::dds::DataReader*>(reader_);
   eprosima::fastdds::rtps::GUID_t guid = reader->guid();
   memcpy(guid_out, guid.guidPrefix.value, 12);
   memcpy(guid_out + 12, guid.entityId.value, 4);
}

void fastddsjava_subscription_matched_status_last_publication_guid(
      const eprosima::fastdds::dds::SubscriptionMatchedStatus& status, uint8_t* guid_out)
{
   eprosima::fastdds::rtps::GUID_t guid;
   eprosima::fastdds::rtps::iHandle2GUID(guid, status.last_publication_handle);
   memcpy(guid_out, guid.guidPrefix.value, 12);
   memcpy(guid_out + 12, guid.entityId.value, 4);
}

void fastddsjava_topic_data_resize(fastddsjava_TopicDataWrapper* data, size_t size)
{
   data->data_vector.resize(size);
}

size_t fastddsjava_topic_data_size(fastddsjava_TopicDataWrapper* data)
{
   return data->data_vector.size();
}

void fastddsjava_topic_data_write(fastddsjava_TopicDataWrapper* data, const uint8_t* src, size_t length)
{
   if (data->data_vector.size() < length)
      data->data_vector.resize(length);
   memcpy(data->data_vector.data(), src, length);
}

void fastddsjava_topic_data_read(fastddsjava_TopicDataWrapper* data, uint8_t* dst, size_t length)
{
   size_t n = std::min(length, data->data_vector.size());
   memcpy(dst, data->data_vector.data(), n);
}

eprosima::fastdds::dds::SampleInfo* fastddsjava_create_sample_info()
{
   return new eprosima::fastdds::dds::SampleInfo();
}

void fastddsjava_delete_sample_info(eprosima::fastdds::dds::SampleInfo* info)
{
   delete info;
}

fastddsjava_DataReaderListener* fastddsjava_create_datareader_listener()
{
   return new fastddsjava_DataReaderListener();
}

void fastddsjava_delete_datareader_listener(fastddsjava_DataReaderListener* listener)
{
   delete listener;
}

void fastddsjava_datareader_listener_set_on_data_available(fastddsjava_DataReaderListener* listener, std::function<void()> callback)
{
   listener->set_on_data_available_callback(std::move(callback));
}

void fastddsjava_datareader_listener_set_on_subscription_matched(fastddsjava_DataReaderListener* listener, std::function<void()> callback)
{
   listener->set_on_subscription_callback(std::move(callback));
}

fastddsjava_DataWriterListener* fastddsjava_create_datawriter_listener()
{
   return new fastddsjava_DataWriterListener();
}

void fastddsjava_delete_datawriter_listener(fastddsjava_DataWriterListener* listener)
{
   delete listener;
}

void fastddsjava_datawriter_listener_set_on_publication_matched(fastddsjava_DataWriterListener* listener, std::function<void()> callback)
{
   listener->set_on_publication_callback(std::move(callback));
}
