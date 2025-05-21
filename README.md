# ais-forwarder-rs

This is a program that scratches a personal itch; i'm using small OpenWRT devices
on friend's boats to develop https://github.com/keesverruijt/mayara and in exchange
I create a tracking page for them.

So I need a way to send location data to my server (over VPN) and it needs to
cache data in case their internet link is down.

This uses https://github.com/canboat/canboat to convert from N2K to NMEA0183, not
Signal K which is what I would recommend for users using a slightly bigger device.

The ais-forwarder takes the NMEA0183 AIS stream out of n2kd and then forwards it to 
services like MarineTraffic, and it takes the RMC message, prepends it with the
MMSI or boatname and then forwards that to my tracking page.

This can support any number of AIS and location services.

ais-forwarder-rs, as the name implies, is written in Rust.


## To use

- Compile or cross-compile to your device architecture (easy in Rust).
- Deploy on target device
- Run it once, it will complain there is no ini file. 
- Copy config.ini.demo to that location and edit it to your satisfaction.
- Now it will run, and it should remain running no matter what happens to the network.
