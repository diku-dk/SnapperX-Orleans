using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class Address : AbstractValue<Address>,  ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public readonly int cityID;

    [Key(1)]
    [Id(1)]
    public readonly string fullAddress;

    [Key(2)]
    [Id(2)]
    public readonly string name;

    [Key(3)]
    [Id(3)]
    public readonly string contact;

    public Address()
    {
        cityID = -1;
        fullAddress = "";
        name = "";
        contact = "";
    }

    public Address(int cityID, string fullAddress, string name, string contact)
    {
        this.cityID = cityID;
        this.fullAddress = fullAddress;
        this.name = name;
        this.contact = contact;
    }

    public object Clone() => new Address(cityID, (string)fullAddress.Clone(), (string)name.Clone(), (string)contact.Clone());

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as Address;
            if (data == null) return false;
            return cityID == data.cityID && fullAddress == data.fullAddress && name == data.name && contact == data.contact;
        }
        catch { return false; }
    }

    public string Print() => $"Address: cityID = {cityID}, fullAddress = {fullAddress}, name = {name}, contact = {contact}";
}