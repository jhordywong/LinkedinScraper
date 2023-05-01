from realestate_com_au import RealestateComAu

api = RealestateComAu()

# Get property listings
listings = api.search(
    locations=["seventeen seventy, qld 4677"],
    channel="buy",
    keywords=["tenant"],
    exclude_keywords=["pool"],
)
print(listings)
print(len(listings))
