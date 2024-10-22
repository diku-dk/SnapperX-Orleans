using Concurrency.Common;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MessagePack;
using System.Diagnostics;

namespace MarketPlace.Grains;

public static class MarketPlaceSerializer
{
    public static (byte[], byte[], byte[], byte[]) SerializeCartActorState(Dictionary<ProductID, ProductInfo> cartItems, Dictionary<ProductID, GrainID> dependencies)
    {
        var item1 = new List<(byte[], byte[])>();
        foreach (var item in cartItems) item1.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        var item2 = new List<(byte[], byte[])>();
        foreach (var item in dependencies) item2.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        return (MessagePackSerializer.Serialize(item1), MessagePackSerializer.Serialize(item2), new byte[0], new byte[0]);
    }

    public static (Dictionary<ProductID, ProductInfo>, Dictionary<ProductID, GrainID>) DeSerializeCartActorState((byte[], byte[], byte[], byte[]) bytes)
    {
        var cartItems = new Dictionary<ProductID, ProductInfo>();
        var item1 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item1);
        foreach (var item in item1) cartItems.Add(MessagePackSerializer.Deserialize<ProductID>(item.Item1), MessagePackSerializer.Deserialize<ProductInfo>(item.Item2));

        var dependencies = new Dictionary<ProductID, GrainID>();
        var item2 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item2);
        foreach (var item in item2) dependencies.Add(MessagePackSerializer.Deserialize<ProductID>(item.Item1), MessagePackSerializer.Deserialize<GrainID>(item.Item2));

        return (cartItems, dependencies);
    }

    public static (byte[], byte[], byte[], byte[]) SerializeOrderActorState(GeneralLongValue nextOrderID, Dictionary<PackageID, PackageInfo> packageInfo, Dictionary<PackageID, GrainID> dependencies, Dictionary<OrderID, OrderInfo> orderInfo)
    {
        var item1 = MessagePackSerializer.Serialize(nextOrderID);

        var item2 = new List<(byte[], byte[])>();
        foreach (var item in packageInfo) item2.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        var item3 = new List<(byte[], byte[])>();
        foreach (var item in dependencies) item3.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        var item4 = new List<(byte[], byte[])>();
        foreach (var item in orderInfo) item4.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        return (item1, MessagePackSerializer.Serialize(item2), MessagePackSerializer.Serialize(item3), MessagePackSerializer.Serialize(item4));
    }

    public static (GeneralLongValue, Dictionary<PackageID, PackageInfo>, Dictionary<PackageID, GrainID>, Dictionary<OrderID, OrderInfo>) DeSerializeOrderActorState((byte[], byte[], byte[], byte[]) bytes)
    {
        var nextOrderID = MessagePackSerializer.Deserialize<GeneralLongValue>(bytes.Item1);

        var packageInfo = new Dictionary<PackageID, PackageInfo>();
        var item2 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item2);
        foreach (var item in item2) packageInfo.Add(MessagePackSerializer.Deserialize<PackageID>(item.Item1), MessagePackSerializer.Deserialize<PackageInfo>(item.Item2));

        var dependencies = new Dictionary<PackageID, GrainID>();
        var item3 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item3);
        foreach (var item in item3) dependencies.Add(MessagePackSerializer.Deserialize<PackageID>(item.Item1), MessagePackSerializer.Deserialize<GrainID>(item.Item2));

        var orderInfo = new Dictionary<OrderID, OrderInfo>();
        var item4 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item4);
        foreach (var item in item4) orderInfo.Add(MessagePackSerializer.Deserialize<OrderID>(item.Item1), MessagePackSerializer.Deserialize<OrderInfo>(item.Item2));

        return (nextOrderID, packageInfo, dependencies, orderInfo);
    }

    public static (byte[], byte[], byte[], byte[]) SerializeProductActorState(Dictionary<ProductID, ProductInfo> items, Dictionary<ProductID, HashSet<GrainID>> followerCartActors, Dictionary<ProductID, HashSet<GrainID>> followerStockActors)
    {
        var item1 = new List<(byte[], byte[])>();
        foreach (var item in items) item1.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        var item2 = new List<(byte[], List<byte[]>)>();
        foreach (var item in followerCartActors)
        {
            var productID = MessagePackSerializer.Serialize(item.Key);
            var itemlist = new List<byte[]>();
            foreach (var iitem in item.Value) itemlist.Add(MessagePackSerializer.Serialize(iitem));
            item2.Add((productID, itemlist));
        }

        var item3 = new List<(byte[], List<byte[]>)>();
        foreach (var item in followerStockActors)
        {
            var productID = MessagePackSerializer.Serialize(item.Key);
            var itemlist = new List<byte[]>();
            foreach (var iitem in item.Value) itemlist.Add(MessagePackSerializer.Serialize(iitem));
            item3.Add((productID, itemlist));
        }

        return (MessagePackSerializer.Serialize(item1), MessagePackSerializer.Serialize(item2), MessagePackSerializer.Serialize(item3), new byte[0]);
    }

    public static (Dictionary<ProductID, ProductInfo>, Dictionary<ProductID, HashSet<GrainID>>, Dictionary<ProductID, HashSet<GrainID>>) DeSerializeProductActorState((byte[], byte[], byte[], byte[]) bytes)
    {
        var items = new Dictionary<ProductID, ProductInfo>();
        var item1 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item1);
        foreach (var item in item1) items.Add(MessagePackSerializer.Deserialize<ProductID>(item.Item1), MessagePackSerializer.Deserialize<ProductInfo>(item.Item2));

        var followerCartActors = new Dictionary<ProductID, HashSet<GrainID>>();
        var item2 = MessagePackSerializer.Deserialize<List<(byte[], List<byte[]>)>>(bytes.Item2);
        foreach (var item in item2)
        {
            var productID = MessagePackSerializer.Deserialize<ProductID>(item.Item1);
            var itemlist = new HashSet<GrainID>();
            foreach (var iitem in item.Item2) itemlist.Add(MessagePackSerializer.Deserialize<GrainID>(iitem));
            followerCartActors.Add(productID, itemlist);
        }

        var followerStockActors = new Dictionary<ProductID, HashSet<GrainID>>();
        var item3 = MessagePackSerializer.Deserialize<List<(byte[], List<byte[]>)>>(bytes.Item3);
        foreach (var item in item3)
        {
            var productID = MessagePackSerializer.Deserialize<ProductID>(item.Item1);
            var itemlist = new HashSet<GrainID>();
            foreach (var iitem in item.Item2) itemlist.Add(MessagePackSerializer.Deserialize<GrainID>(iitem));
            followerStockActors.Add(productID, itemlist);
        }

        return (items, followerCartActors, followerStockActors);
    }

    public static (byte[], byte[], byte[], byte[]) SerializeShipmentActorState(GeneralLongValue nextPackageID, Dictionary<PackageID, PackageInfo> packages, Dictionary<PackageID, GrainID> dependencies)
    {
        var item1 = MessagePackSerializer.Serialize(nextPackageID);

        var item2 = new List<(byte[], byte[])>();
        foreach (var item in packages) item2.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        var item3 = new List<(byte[], byte[])>();
        foreach (var item in dependencies) item3.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        return (item1, MessagePackSerializer.Serialize(item2), MessagePackSerializer.Serialize(item3), new byte[0]);
    }

    public static (GeneralLongValue, Dictionary<PackageID, PackageInfo>, Dictionary<PackageID, GrainID>) DeSerializeShipmentActorState((byte[], byte[], byte[], byte[]) bytes)
    {
        var nextPackageID = MessagePackSerializer.Deserialize<GeneralLongValue>(bytes.Item1);

        var packages = new Dictionary<PackageID, PackageInfo>();
        var item2 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item2);
        foreach (var item in item2) packages.Add(MessagePackSerializer.Deserialize<PackageID>(item.Item1), MessagePackSerializer.Deserialize<PackageInfo>(item.Item2));

        var dependencies = new Dictionary<PackageID, GrainID>();
        var item3 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item3);
        foreach (var item in item3) dependencies.Add(MessagePackSerializer.Deserialize<PackageID>(item.Item1), MessagePackSerializer.Deserialize<GrainID>(item.Item2));

        return (nextPackageID, packages, dependencies);
    }

    public static (byte[], byte[], byte[], byte[]) SerializeStockActorState(Dictionary<ProductID, StockInfo> stocks, Dictionary<ProductID, GrainID> dependencies)
    {
        var item1 = new List<(byte[], byte[])>();
        foreach (var item in stocks) item1.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        var item2 = new List<(byte[], byte[])>();
        foreach (var item in dependencies) item2.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        return (MessagePackSerializer.Serialize(item1), MessagePackSerializer.Serialize(item2), new byte[0], new byte[0]);
    }

    public static (Dictionary<ProductID, StockInfo>, Dictionary<ProductID, GrainID>) DeSerializeStockActorState((byte[], byte[], byte[], byte[]) bytes)
    {
        var stocks = new Dictionary<ProductID, StockInfo>();
        var item1 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item1);
        foreach (var item in item1) stocks.Add(MessagePackSerializer.Deserialize<ProductID>(item.Item1), MessagePackSerializer.Deserialize<StockInfo>(item.Item2));

        var dependencies = new Dictionary<ProductID, GrainID>();
        var item2 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item2);
        foreach (var item in item2) dependencies.Add(MessagePackSerializer.Deserialize<ProductID>(item.Item1), MessagePackSerializer.Deserialize<GrainID>(item.Item2));

        return (stocks, dependencies);
    }

    public static void CheckStateCorrectnessForOrleans(Dictionary<GrainID, (byte[], byte[], byte[], byte[])> masterGrains)
    {
        var cartActors_cartItems = new Dictionary<GrainID, Dictionary<ProductID, ProductInfo>>();
        var cartActors_dependencies = new Dictionary<GrainID, Dictionary<ProductID, GrainID>>();
        var orderActors_packageInfo = new Dictionary<GrainID, Dictionary<PackageID, PackageInfo>>();
        var orderActors_dependencies = new Dictionary<GrainID, Dictionary<PackageID, GrainID>>();
        var orderActors_orderInfo = new Dictionary<GrainID, Dictionary<OrderID, OrderInfo>>();
        var productActors_items = new Dictionary<GrainID, Dictionary<ProductID, ProductInfo>>();
        var productActors_followerCartActors = new Dictionary<GrainID, Dictionary<ProductID, HashSet<GrainID>>>();
        var productActors_followerStockActors = new Dictionary<GrainID, Dictionary<ProductID, HashSet<GrainID>>>();
        var shipmentActors_packages = new Dictionary<GrainID, Dictionary<PackageID, PackageInfo>>();
        var shipmentActors_dependencies = new Dictionary<GrainID, Dictionary<PackageID, GrainID>>();
        var stockActors_stocks = new Dictionary<GrainID, Dictionary<ProductID, StockInfo>>();
        var stockActors_dependencies = new Dictionary<GrainID, Dictionary<ProductID, GrainID>>();

        foreach (var item in masterGrains)
        {
            var grainID = item.Key;

            if (grainID.className.Contains("CartActor"))
            {
                (var cartItems, var dependencies) = DeSerializeCartActorState(item.Value);
                cartActors_cartItems.Add(grainID, cartItems);
                cartActors_dependencies.Add(grainID, dependencies);
            }
            else if (grainID.className.Contains("OrderActor"))
            {
                (_, var packageInfo, var dependencies, var orderInfo) = DeSerializeOrderActorState(item.Value);
                orderActors_packageInfo.Add(grainID, packageInfo);
                orderActors_dependencies.Add(grainID, dependencies);
                orderActors_orderInfo.Add(grainID, orderInfo);
            }
            else if (grainID.className.Contains("ProductActor"))
            {
                (var items, var followerCartActors, var followerStockActors) = DeSerializeProductActorState(item.Value);
                productActors_items.Add(grainID, items);
                productActors_followerCartActors.Add(grainID, followerCartActors);
                productActors_followerStockActors.Add(grainID, followerStockActors);
            }
            else if (grainID.className.Contains("ShipmentActor"))
            {
                (_, var packages, var dependencies) = DeSerializeShipmentActorState(item.Value);
                shipmentActors_packages.Add(grainID, packages);
                shipmentActors_dependencies.Add(grainID, dependencies);
            }
            else if (grainID.className.Contains("StockActor"))
            {
                (var stocks, var dependencies) = DeSerializeStockActorState(item.Value);
                stockActors_stocks.Add(grainID, stocks);
                stockActors_dependencies.Add(grainID, dependencies);
            }
        }

        // STEP 1: check the dependency between product and stock actors
        foreach (var stockActorInfo in stockActors_stocks)
        {
            var stockActorID = stockActorInfo.Key;
            var stocks = stockActorInfo.Value;
            var dependencies = stockActors_dependencies[stockActorID];

            foreach (var stock in stocks)
            {
                var productID = stock.Key;

                // a product in the stock actor should have a dependency registered
                Debug.Assert(dependencies.ContainsKey(productID));
                var productActorID = dependencies[productID];
                dependencies.Remove(productID);

                // a product in the stock actor should exist in a product actor
                Debug.Assert(productActors_items.ContainsKey(productActorID));
                Debug.Assert(productActors_items[productActorID].ContainsKey(productID));

                // the product actor should have this dependency registered as well
                Debug.Assert(productActors_followerStockActors.ContainsKey(productActorID));
                Debug.Assert(productActors_followerStockActors[productActorID].ContainsKey(productID));
                Debug.Assert(productActors_followerStockActors[productActorID][productID].Contains(stockActorID));
                productActors_followerStockActors[productActorID][productID].Remove(stockActorID);
            }

            Debug.Assert(dependencies.Count == 0);
        }

        foreach (var productActorInfo in productActors_followerStockActors) foreach (var dependency in productActorInfo.Value) Debug.Assert(dependency.Value.Count == 0);

        Console.WriteLine($"CheckDataConsistency: {stockActors_stocks.Count} stock actors are consistent with product actors. ");

        // STEP 2: check the dependency between product and cart actors
        foreach (var cartActorInfo in cartActors_cartItems)
        {
            var cartActorID = cartActorInfo.Key;
            var cartItems = cartActorInfo.Value;
            var dependencies = cartActors_dependencies[cartActorID];

            foreach (var item in cartItems)
            {
                var productID = item.Key;
                var productInfo = item.Value;

                // a product in the cart actor should have a dependency registered
                Debug.Assert(dependencies.ContainsKey(productID));
                var productActorID = dependencies[productID];
                dependencies.Remove(productID);

                // a product in the cart actor should exist in a product actor
                Debug.Assert(productActors_items.ContainsKey(productActorID));
                Debug.Assert(productActors_items[productActorID].ContainsKey(productID));

                // the product in cart actor should have the same value as in the product actor
                Debug.Assert(productInfo.Equals(productActors_items[productActorID][productID]));

                // the product actor should have this dependency registered as well
                Debug.Assert(productActors_followerCartActors.ContainsKey(productActorID));
                Debug.Assert(productActors_followerCartActors[productActorID].ContainsKey(productID));
                Debug.Assert(productActors_followerCartActors[productActorID][productID].Contains(cartActorID));
                productActors_followerCartActors[productActorID][productID].Remove(cartActorID);
            }

            Debug.Assert(dependencies.Count == 0);
        }

        foreach (var productActorInfo in productActors_followerCartActors) foreach (var dependency in productActorInfo.Value) Debug.Assert(dependency.Value.Count == 0);

        Console.WriteLine($"CheckDataConsistency: {cartActors_cartItems.Count} cart actors are consistent with product actors. ");

        // STEP 3: check dependency between shipment and order actors
        foreach (var orderActorInfo in orderActors_packageInfo)
        {
            var orderActorID = orderActorInfo.Key;
            var packageInfo = orderActorInfo.Value;
            var dependencies = orderActors_dependencies[orderActorID];
            var orderInfo = orderActors_orderInfo[orderActorID];

            foreach (var package in packageInfo)
            {
                var packageID = package.Key;
                var info = package.Value;

                // a package in the order actor should have a dependency registerd
                Debug.Assert(dependencies.ContainsKey(packageID));
                var shipmentActorID = dependencies[packageID];
                dependencies.Remove(packageID);

                // the package should exist in the shipment actor
                Debug.Assert(shipmentActors_packages.ContainsKey(shipmentActorID));
                Debug.Assert(shipmentActors_packages[shipmentActorID].ContainsKey(packageID));

                // the package info should be the same
                Debug.Assert(info.Equals(shipmentActors_packages[shipmentActorID][packageID]));

                // the shipment actor should have the dependency registered
                Debug.Assert(shipmentActors_dependencies.ContainsKey(shipmentActorID));
                Debug.Assert(shipmentActors_dependencies[shipmentActorID].ContainsKey(packageID));
                Debug.Assert(shipmentActors_dependencies[shipmentActorID][packageID].Equals(orderActorID));
                shipmentActors_dependencies[shipmentActorID].Remove(packageID);

                // check the order info consists this package info
                var orderID = info.orderID;
                Debug.Assert(orderInfo.ContainsKey(orderID));
                Debug.Assert(orderInfo[orderID].packageIDs.Contains(packageID));
                orderInfo[orderID].packageIDs.Remove(packageID);
            }

            Debug.Assert(dependencies.Count == 0);

            foreach (var info in orderInfo) Debug.Assert(info.Value.packageIDs.Count == 0);
        }

        foreach (var shipmentActorInfo in shipmentActors_dependencies) Debug.Assert(shipmentActorInfo.Value.Count == 0);

        Console.WriteLine($"CheckDataConsistency: {orderActors_packageInfo.Count} order actors are consistent with shipment actors. ");
    }
}