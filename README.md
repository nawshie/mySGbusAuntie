
# mySGbusAuntie

***Disclaimer:*** this is my first project on this, I am by no means an expert and just do these fun projects on the side to challenge myself.

I’ve forked and adapted the code from &lt;> to create my own version of the BusAuntie.

This project took me some time to "decode" what was being done and adapt it to what I was trying to do which was

1) In the mornings when my kids go to school they (like to original author of the code) need to know the buses and bus arrival times at the nearest bus stops where we live.
2) In addition because they take the train (subway / MRT) after the bus they need to know if there any distruptions along the route.
3) Once they have gone I dont need the display to show the bus times any more but rather some useful information, so I chose to display a dashboard from my homeassistant instance that shows weather for today, the 5 day outlook and also the calendar entries for today and the next day

---

## Hardware 
(use the available sources in your country, for me it was a combination of amazon, lazada, shopee, cytron, aliexpress)
  1) Raspberry Pi Zero W (link later) with GPIO header pins
  2) Waveshare 7.5inch e-ink B/W/R (link later)
  3) HAT for Waveshare with GPIO interface for RPi (this makes it easier to install without soldering)
  4) IKEA picture frame (link later)
  5) Access to a 3D printer to print the parts needed (frame for display to sit on, internal frame and backing to hold frame down and for pi to sit on - link to follow for my STLs) 
  6) Power supply and cable for RPi in 1)

# Software
  1) All written in Pi and referncing the libraries from the original fork plus the waveshare libraries for the display
  2) You will need an AIP key to access the data from the LTA via their DataMall (link here)
  3) Assume you runnng a homeassistant instance in your home
  4) Install the Graphite Theme (link) and also Puppeteer add-in (link) on the homeassistant instance 

## Installation
  1) Follow the original authors suggestions to install the libraries and basic code
  2) Change the main.py to this version
  3) Setup your dashboard in home assistant and test the look and feel through your browser using the instructions in the Puppeteer add-on Github
  4) Adjust the parameter/config file to suit your situation [API key, bus stop codes, homeassistant instance, dashboard url, etc]
  5) Follow the original authors code to setup the file as a service to run when the Rpi boots

## Disclaimers
1) This was a weekend project and i am not a professional software engineer !
2) Yes there is optimisation to this code - happy for suggestions etc but will be updating as and when I get the time

## Todos:
1) Create a web config to adjust the parameters and then reload
2) Adjust the fonts and drawings to be better
3) Change UI such that if only 1 bus stop is configured then the train distruptions are displayed on the right
4) Update to show journey time to a specific location if catching the preferred bus
5) others ...


-----

**Current Version: v12.0**  
**Status: Production Ready** ✅  
**Last Updated: October 2, 2025**  
**Latest Feature: Atkinson Hyperlegible Typography** 🔤

*For detailed code, see the main artifact.*  
*For issues or questions, refer to systemd service logs.*
