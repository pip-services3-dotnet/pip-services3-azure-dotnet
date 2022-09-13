using PipServices3.Commons.Commands;
using PipServices3.Commons.Data;
using PipServices3.Commons.Run;
using PipServices3.Commons.Validate;
using System;

namespace PipServices3.Azure
{
    public class DummyCommandSet : CommandSet
    {
        private IDummyController _controller;

        public DummyCommandSet(IDummyController controller)
        {
            this._controller = controller;

            AddCommand(MakeGetPageByFilterCommand());
            AddCommand(MakeGetOneByIdCommand());
            AddCommand(MakeCreateCommand());
            AddCommand(MakeUpdateCommand());
            AddCommand(MakeDeleteByIdCommand());
        }

        private ICommand MakeGetPageByFilterCommand()
        {
            return new Command(
                "get_dummies",

                new ObjectSchema()
                    .WithOptionalProperty("filter", new FilterParamsSchema())
                    .WithOptionalProperty("paging", new PagingParamsSchema()),
                async (string correlationId, Parameters args) =>
                {
                    var filter = FilterParams.FromValue(args.Get("filter"));
                    var paging = PagingParams.FromValue(args.Get("paging"));
                    return await _controller.GetPageByFilterAsync(correlationId, filter, paging);
                }
            );
        }

        private ICommand MakeGetOneByIdCommand()
        {
            return new Command(
                "get_dummy_by_id",

                new ObjectSchema()
                    .WithRequiredProperty("dummy_id", TypeCode.String),
                async (string correlationId, Parameters args) =>
                {
                    var id = args.GetAsString("dummy_id");
                    return await _controller.GetOneByIdAsync(correlationId, id);
                }
            );
        }

        private ICommand MakeCreateCommand()
        {
            return new Command(
                "create_dummy",

                new ObjectSchema()
                    .WithRequiredProperty("dummy", new DummySchema()),
                async (string correlationId, Parameters args) =>
                {
                    Dummy entity = ExtractDummy(args);
                    return await _controller.CreateAsync(correlationId, entity);
                }
            );
        }

        private ICommand MakeUpdateCommand()
        {
            return new Command(
                "update_dummy",

                new ObjectSchema()
                    .WithRequiredProperty("dummy", new DummySchema()),
                async (string correlationId, Parameters args) =>
                {
                    Dummy entity = ExtractDummy(args);
                    return await _controller.UpdateAsync(correlationId, entity);
                }
            );
        }

        private ICommand MakeDeleteByIdCommand()
        {
            return new Command(
                "delete_dummy",

                new ObjectSchema()
                    .WithRequiredProperty("dummy_id", TypeCode.String),
                async (string correlationId, Parameters args) =>
                {
                    var id = args.GetAsString("dummy_id");
                    return await _controller.DeleteByIdAsync(correlationId, id);
                }
            );
        }


        private static Dummy ExtractDummy(Parameters args)
        {
            var map = args.GetAsMap("dummy");

            var id = map.GetAsStringWithDefault("id", string.Empty);
            var key = map.GetAsStringWithDefault("key", string.Empty);
            var content = map.GetAsStringWithDefault("content", string.Empty);

            var dummy = new Dummy(id, key, content);
            return dummy;
        }
    }
}