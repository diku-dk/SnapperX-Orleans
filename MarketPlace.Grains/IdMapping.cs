using Utilities;

namespace MarketPlace.Grains;

public static class IdMapping
{
    // ===============================================================================================
    public static Guid GetSellerActorGuid(int sellerID, int baseCityID) => Helper.ConvertIntToGuid(sellerID, baseCityID);

    public static Guid GetProductActorGuid(int sellerID, int baseCityID) => Helper.ConvertIntToGuid(sellerID, baseCityID);

    public static Guid GetStockActorGuid(int sellerID, int cityID) => Helper.ConvertIntToGuid(sellerID, cityID);

    public static Guid GetShipmentActorGuid(int sellerID, int cityID) => Helper.ConvertIntToGuid(sellerID, cityID);

    // ===============================================================================================
    public static Guid GetCustomerActorGuid(int customerID, int baseCityID) => Helper.ConvertIntToGuid(customerID, baseCityID);

    public static Guid GetCartActorGuid(int customerID, int baseCityID) => Helper.ConvertIntToGuid(customerID, baseCityID);

    public static Guid GetPaymentActorGuid(int customerID, int baseCityID) => Helper.ConvertIntToGuid(customerID, baseCityID);

    public static Guid GetOrderActorGuid(int customerID, int baseCityID) => Helper.ConvertIntToGuid(customerID, baseCityID);

    // ===============================================================================================
    public static int GetCityID(int regionIndex, int siloIndex, int cityIndex, int numSiloPerRegion, int numCityPerSilo)
        => regionIndex * numSiloPerRegion * numCityPerSilo + siloIndex * numCityPerSilo + cityIndex;

    public static int GetSellerID(int siloIndex, int cityIndex, int sellerIndex, int numSiloPerRegion, int numCityPerSilo, int numSellerPerCity)
        => siloIndex * numCityPerSilo * numSellerPerCity + cityIndex * numSellerPerCity + sellerIndex;
}