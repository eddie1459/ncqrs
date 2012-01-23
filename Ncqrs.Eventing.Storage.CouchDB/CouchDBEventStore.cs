
using System;
using System.Linq;
using RedBranch.Hammock;

namespace Ncqrs.Eventing.Storage.CouchDB
{
    public class CouchDbEventStore : IEventStore
    {
        private readonly Session _session;

        public CouchDbEventStore(string database)
        {
            var c = new Connection(new Uri("http://localhost:5984"));
            _session = c.CreateSession(database);
        }

        public CommittedEventStream ReadFrom(System.Guid id, long minVersion, long maxVersion)
        {
            var storedEvents = new Repository<StoredEvent>(_session).All()
                .Where(x => x.EventSourceId == id)
                .Where(x => x.EventSequence >= minVersion)
                .Where(x => x.EventSequence <= maxVersion)
                .ToList().OrderBy(x => x.EventSequence);
            return new CommittedEventStream(id, storedEvents.Select(ToComittedEvent));
        }

        private static CommittedEvent ToComittedEvent(StoredEvent x)
        {
            return new CommittedEvent(x.CommitId, x.EventIdentifier, x.EventSourceId, x.EventSequence, x.EventTimeStamp, x.Data, x.Version);
        }

        public void Store(UncommittedEventStream eventStream)
        {
            foreach (var uncommittedEvent in eventStream)
            {
                _session.Save(ToStoredEvent(eventStream.CommitId, uncommittedEvent));
            }
        }

        private static StoredEvent ToStoredEvent(Guid commitId, UncommittedEvent uncommittedEvent)
        {
            return new StoredEvent
            {
                Id = uncommittedEvent.EventSourceId + "/" + uncommittedEvent.EventSequence,
                EventIdentifier = uncommittedEvent.EventIdentifier,
                EventTimeStamp = uncommittedEvent.EventTimeStamp,
                Version = uncommittedEvent.EventVersion,
                CommitId = commitId,
                Data = uncommittedEvent.Payload,
                EventSequence = uncommittedEvent.EventSequence,
                EventSourceId = uncommittedEvent.EventSourceId,
            };
        }

    }
}
