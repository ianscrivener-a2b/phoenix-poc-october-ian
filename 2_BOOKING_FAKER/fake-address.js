const get_fake_address_pair = function() {
  const states = [
    { name: 'New South Wales', abbreviation: 'NSW' },
    { name: 'Victoria', abbreviation: 'VIC' },
    { name: 'Queensland', abbreviation: 'QLD' },
    { name: 'Western Australia', abbreviation: 'WA' },
    { name: 'South Australia', abbreviation: 'SA' },
    { name: 'Tasmania', abbreviation: 'TAS' },
    { name: 'Australian Capital Territory', abbreviation: 'ACT' },
    { name: 'Northern Territory', abbreviation: 'NT' }
  ];

  const streetTypes = ['Street', 'Road', 'Avenue', 'Lane', 'Drive', 'Court', 'Place', 'Crescent', 'Way'];

  const suburbs = {
    NSW: ['Sydney', 'Newcastle', 'Wollongong', 'Dubbo', 'Bathurst'],
    VIC: ['Melbourne', 'Geelong', 'Ballarat', 'Bendigo', 'Shepparton'],
    QLD: ['Brisbane', 'Gold Coast', 'Cairns', 'Townsville', 'Toowoomba'],
    WA: ['Perth', 'Fremantle', 'Bunbury', 'Geraldton', 'Albany'],
    SA: ['Adelaide', 'Mount Gambier', 'Whyalla', 'Port Augusta', 'Victor Harbor'],
    TAS: ['Hobart', 'Launceston', 'Devonport', 'Burnie', 'Kingston'],
    ACT: ['Canberra', 'Belconnen', 'Tuggeranong', 'Gungahlin', 'Woden Valley'],
    NT: ['Darwin', 'Alice Springs', 'Palmerston', 'Katherine', 'Nhulunbuy']
  };

  function generateStreetName() {
    const adjectives = ['Green', 'Blue', 'Red', 'White', 'Black', 'Golden', 'Silver', 'Sunny', 'Shady', 'Windy'];
    const nouns = ['Hill', 'Creek', 'River', 'Lake', 'Forest', 'Mountain', 'Valley', 'Ridge', 'Meadow', 'Beach'];
    return `${adjectives[Math.floor(Math.random() * adjectives.length)]} ${nouns[Math.floor(Math.random() * nouns.length)]}`;
  }

  function generateSingleAddress(state) {
    const randomSuburb = suburbs[state.abbreviation][Math.floor(Math.random() * suburbs[state.abbreviation].length)];
    
    return {
      houseNumber: Math.floor(Math.random() * 300) + 1,
      streetName: generateStreetName(),
      streetType: streetTypes[Math.floor(Math.random() * streetTypes.length)],
      suburb: randomSuburb,
      postcode: Math.floor(Math.random() * 9000) + 1000,
      state: state.name,
      stateAbbreviation: state.abbreviation
    };
  }

  function formatAddress(address) {
    return `${address.houseNumber} ${address.streetName} ${address.streetType}, ${address.suburb}, ${address.state} ${address.stateAbbreviation} ${address.postcode}`;
  }

  const randomState = states[Math.floor(Math.random() * states.length)];
  const origin = generateSingleAddress(randomState);
  const destination = generateSingleAddress(randomState);

  return {
    origin: formatAddress(origin),
    destination: formatAddress(destination)
  };
}

// Usage example
// const addressPair = get_fake_address_pair();
// console.log(JSON.stringify(addressPair, null, 2));

export { get_fake_address_pair };
