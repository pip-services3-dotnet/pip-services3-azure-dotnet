using PipServices3.Commons.Commands;
using PipServices3.Commons.Data;

using System.Collections.Generic;
using System.Threading.Tasks;

namespace PipServices3.Azure
{
    public class DummyController: IDummyController, ICommandable
    {
        private readonly object _lock = new object();
        private DummyCommandSet _commandSet;
        private readonly List<Dummy> _entities = new();

        public CommandSet GetCommandSet()
        {
            if (this._commandSet == null)
                this._commandSet = new DummyCommandSet(this);
            return this._commandSet;
        }

        public async Task<DataPage<Dummy>> GetPageByFilterAsync(string correlationId, FilterParams filter, PagingParams paging)
        {
            await Task.Delay(0);

            filter = filter != null ? filter : new FilterParams();
            string key = filter.GetAsNullableString("key");

            paging = paging != null ? paging : new PagingParams();
            var skip = paging.GetSkip(0);
            var take = paging.GetTake(100);

            List<Dummy> result = new();

            lock (_lock)
            {
                for (var i = 0; i < this._entities.Count; i++)
                {
                    Dummy entity = _entities[i];
                    if (key != null && key != entity.Key)
                        continue;

                    skip--;
                    if (skip >= 0) continue;

                    take--;
                    if (take < 0) break;

                    result.Add(entity);
                }
            }

            return new DataPage<Dummy>(result);
        }

        public async Task<Dummy> GetOneByIdAsync(string correlationId, string id)
        {
            await Task.Delay(0);

            lock (_lock)
            {
                foreach (var entity in _entities)
                {
                    if (entity.Id.Equals(id))
                        return entity;
                }
            }

            return null;
        }

        public async Task<Dummy> CreateAsync(string correlationId, Dummy entity)
        {
            await Task.Delay(0);

            lock (_lock)
            {
                if (string.IsNullOrEmpty(entity.Id))
                    entity.Id = IdGenerator.NextLong();

                _entities.Add(entity);
            }
            return entity;
        }

        public async Task<Dummy> UpdateAsync(string correlationId, Dummy newEntity)
        {
            await Task.Delay(0);

            lock (_lock)
            {
                for (int index = 0; index < _entities.Count; index++)
                {
                    var entity = _entities[index];
                    if (entity.Id.Equals(newEntity.Id))
                    {
                        _entities[index] = newEntity;
                        return newEntity;
                    }
                }
            }
            return null;
        }

        public async Task<Dummy> DeleteByIdAsync(string correlationId, string id)
        {
            await Task.Delay(0);

            lock (_lock)
            {
                for (int index = 0; index < _entities.Count; index++)
                {
                    var entity = _entities[index];
                    if (entity.Id.Equals(id))
                    {
                        _entities.RemoveAt(index);
                        return entity;
                    }
                }
            }
            return null;
        }
    }
}