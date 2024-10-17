import fetch from 'node-fetch';
import { get_fake_address_pair } from './fake-address.js'

const API_URL                   = 'https://jobapieg001devae-ekb8djhwfjdabveh.australiaeast-01.azurewebsites.net/booking'; 
const bookings_per_second       = 10;
const AUTH_KEY                  = 'Kn57ida0SUQ6ooWXuf6bF8JIsQdrNrqrV0lmXSGZmWy03qFZrrpG0H4nZM00gaTo';

function make_booking() {
    // This function should return the JSON data for the booking
    // Modify this to suit your specific needs
    let addr = get_fake_address_pair();
    return {
        "tenantId": "happycabs",
        "fleetId": "fleet001",
        "jobTime": "string",
        "jobType": "string",
        "customerId": "string",
        "bookingRef": "string",
        "destination": addr.destination,
        "origin": addr.origin,
        "correlationId": "string"
      }
}

async function postBooking() {
    try {
        const bookingData = make_booking();
        const response = await fetch(API_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'api-shared-key': AUTH_KEY
            },
            body: JSON.stringify(bookingData),
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();
        console.log('Booking posted successfully:', result);
    } catch (error) {
        console.error('Error posting booking:', error);
    }
}

function startPostingBookings() {
    // Post immediately on start
    postBooking();

    // Then post every 10 seconds
    setInterval(postBooking, bookings_per_second * 1000);
}

startPostingBookings();
