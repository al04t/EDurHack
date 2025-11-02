var yearSlider = document.getElementById("yearSlider");
var output = document.getElementById("value");
var button = document.getElementById("dataButton");
output.innerHTML = yearSlider.value;
initialYear = Number(yearSlider.value);
maxYear = Number(yearSlider.max);
fetchYear(yearSlider.value);
let yearSums = [];

yearSlider.oninput = function() {
    output.innerHTML = this.value;
    fetchYear(this.value);
}



function fetchYear(year) {
    address = '/Dataset/cleanData/woodchuck_forecast_hundreds.csv';
    console.log('Fetching from:', address);
    fetch(address)
        .then(response => response.text())
        .then(csvContent => {
            Papa.parse(csvContent, {
                header: true,
                dynamicTyping: true,
                complete: function(results) {
                    dataObject = results.data;
                    var testData = {max: 15000, data: []}
                    console.log(dataObject)
                    for (let i = 0; i < dataObject.length - 1; i++) {
                        if (dataObject[i].year == year) {
                            var filteredData = {
                                lat: dataObject[i].latitude,
                                lng: dataObject[i].longitude,
                                value: dataObject[i].total_wood_chucked_lbs
                            }
                        testData.data.push(filteredData)
                        }
                    }
                    console.log(testData.data)
                    heatmapLayer.setData(testData);
                }
            });
        })
    .catch(error => console.error('Error reading CSV:', error));
}

async function sumYears(year, all) {
    const promises = [];
    var sumGlobal = 0;
    if (!all) {
    for (let i = year; i <= maxYear; i = i + 20) {
        console.log("iterate")
        address = '/Dataset/cleanData/woodchuck_forecast_hundreds.csv';
        console.log('Fetching from:', address);
        const promise = fetch(address)
            .then(response => response.text())
            .then(csvContent => {
                return new Promise((resolve, reject) => {
                Papa.parse(csvContent, {
                    header: true,
                    dynamicTyping: true,
                    complete: function(results) {
                        dataObject = results.data;
                        var sum = 0;
                        for (let j = 0; j < dataObject.length - 1; j++) {
                            if (dataObject[j].year == i) {
                                console.log("summing")
                                sum = sum + dataObject[j].wood_chucked_per_woodchuck_lbs
                            }
                        }
                        yearSums.push({year: i, sum: sum})
                        resolve();
                    }
                });
            })
            })
        .catch(error => console.error('Error reading CSV:', error));
        promises.push(promise);
    }
    await Promise.all(promises);
    yearSums.sort((a, b) => a.year - b.year);
    return yearSums;

    } else if (all) {
        console.log("iterate")
        address = '/Dataset/cleanData/woodchuck_forecast_hundreds.csv';
        console.log('Fetching from:', address);
        const promise = fetch(address)
            .then(response => response.text())
            .then(csvContent => {
                return new Promise((resolve, reject) => {
                Papa.parse(csvContent, {
                    header: true,
                    dynamicTyping: true,
                    complete: function(results) {
                        dataObject = results.data;
                        for (let j = 0; j < dataObject.length - 1; j++) {
                            sumGlobal = sumGlobal + dataObject[j].wood_chucked_per_woodchuck_lbs
                        }
                        resolve();
                    }
                });
            })
            })
        .catch(error => console.error('Error reading CSV:', error));
        promises.push(promise);
    }

    await Promise.all(promises);
    return sumGlobal;
}

var baseLayer = L.tileLayer('https://tiles.stadiamaps.com/tiles/stamen_toner/{z}/{x}/{y}{r}.{ext}', {
	minZoom: 7,
	maxZoom: 10,
	attribution: '&copy; <a href="https://www.stadiamaps.com/" target="_blank">Stadia Maps</a> &copy; <a href="https://www.stamen.com/" target="_blank">Stamen Design</a> &copy; <a href="https://openmaptiles.org/" target="_blank">OpenMapTiles</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
	ext: 'png'
});

var cfg = {
    radius: 0.3,
    blur: 0.9,
    maxOpacity: 0.5,
    scaleRadius: true,
    useLocalExtrema: false,
    latField: 'lat',
    lngField: 'lng',
    valueField: 'value',
    gradient: {
        0.0: '#ffee00',
        0.2: '#ffbb00',
        0.4: '#ff7700',
        0.6: '#ff3300',
        0.8: '#cc3300',
        1.0: '#aa0000'
    }
};

var heatmapLayer = new HeatmapOverlay(cfg);

const borderCoordinates = [
    [42.269235, -79.761890],
    [41.976550, -80.519823],
    [39.719341, -80.519823],
    [39.721087, -75.774022],
    [39.771177, -75.741020],
    [39.808866, -75.689791],
    [39.834818, -75.604773],
    [39.832307, -75.495774],
    [39.801330, -75.414026],
    [39.845067, -75.332409],
    [39.886949, -75.139784],
    [40.014571, -75.038047],
    [40.151381, -74.726670],
    [40.333598, -74.942056],
    [40.406411, -74.975968],
    [40.415800, -75.062291],
    [40.547116, -75.071539],
    [40.573626, -75.124087],
    [40.563992, -75.168790],
    [40.580140, -75.195973],
    [40.759264, -75.188557],
    [40.858264, -75.053399],
    [40.983452, -75.136237],
    [41.354321, -74.695885],
    [41.427325, -74.741646],
    [41.505030, -75.008666],
    [41.809122, -75.076069],
    [41.849688, -75.143472],
    [41.868996, -75.257539],
    [41.998210, -75.345682],
    [41.998210, -79.761653]
];

var polygon = L.polygon(borderCoordinates, {
    color: '#880000',
    weight: 3,
    fillOpacity: 0,
});

var map = new L.Map('map', {
    center: new L.LatLng(41.203323, -77.194527),
    zoom: 7,
    layers: [polygon, baseLayer, heatmapLayer]
});

function plotXYchart(list) {
    const years = list.map(item => item.year);
    const wood = list.map(item => item.sum);

    const data = {
        labels: years,
        datasets: [{
            label: 'Wood chucked in a specific area',
            data: wood,
            borderColor: '#DA3434',
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            fill: false,
        }]
    };

    const config = {
        type: 'line',
        data: data,
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Year'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'Wood chucked'
                    }
                }
            }
        }
    };
    try{
        if(window.xyChart && typeof window.xyChart.destroy === 'function'){
            window.xyChart.destroy();
        }
    }catch(e){ console.warn('Failed to destroy previous xyChart', e); }
    window.xyChart = new Chart(
        document.getElementById('XYchart'),
        config
    );
}

    try{
        if (!window.barChart) window.barChart = null;
    }catch(e){}




async function plotXYchartForCoord(lat, lon, radiusDeg = 0.05) {
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
    const address = '/Dataset/cleanData/woodchuck_forecast_hundreds.csv';
    try {
        const resp = await fetch(address);
        const csv = await resp.text();
        return new Promise((resolve)=>{
            Papa.parse(csv, { header: true, dynamicTyping: true, complete(results){
                const data = results.data || [];
                const r2 = radiusDeg * radiusDeg;
                const yearMap = new Map();

                for (let i=0;i<data.length;i++){
                    const row = data[i];
                    if (!row) continue;
                    const rlat = Number(row.latitude);
                    const rlon = Number(row.longitude);
                    const y = Number(row.year);
                    if (!Number.isFinite(rlat) || !Number.isFinite(rlon) || !Number.isFinite(y)) continue;
                    const dlat = rlat - lat;
                    const dlon = rlon - lon;
                    if ((dlat*dlat + dlon*dlon) <= r2){
                        const val = Number(row.total_wood_chucked_lbs ?? row.wood_chucked_per_woodchuck_lbs ?? 0) || 0;
                        yearMap.set(y, (yearMap.get(y) || 0) + val);
                    }
                }

                let list = Array.from(yearMap.entries()).map(([year,sum])=>({ year: Number(year), sum: sum }));
                list.sort((a,b)=>a.year - b.year);


                if (list.length === 0){
                    const best = new Map(); 
                    for (let i=0;i<data.length;i++){
                        const row = data[i]; if(!row) continue;
                        const rlat = Number(row.latitude); const rlon = Number(row.longitude); const y = Number(row.year);
                        if (!Number.isFinite(rlat) || !Number.isFinite(rlon) || !Number.isFinite(y)) continue;
                        const d2 = (rlat-lat)*(rlat-lat) + (rlon-lon)*(rlon-lon);
                        const val = Number(row.total_wood_chucked_lbs ?? row.wood_chucked_per_woodchuck_lbs ?? 0) || 0;
                        const cur = best.get(y);
                        if (!cur || d2 < cur.dist){ best.set(y, { dist: d2, val }); }
                    }
                    list = Array.from(best.entries()).map(([year,obj])=>({ year: Number(year), sum: obj.val }));
                    list.sort((a,b)=>a.year - b.year);
                }

            
                try{ plotXYchart(list); }catch(e){ console.error('plotXYchart failed', e); }
                resolve(list);
            }});
        });
    } catch (e) {
        console.error('Error fetching CSV for coord plot', e);
        return [];
    }
}


window.addEventListener('load', ()=>{
    try{
        const latInput = document.getElementById('dashLat');
        const lonInput = document.getElementById('dashLon');
        function pointInPoly(lat, lon, poly){
            let x = lon, y = lat;
            let inside = false;
            for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
                const xi = poly[i][1], yi = poly[i][0];
                const xj = poly[j][1], yj = poly[j][0];
                const intersect = ((yi > y) != (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi + 0.0) + xi);
                if (intersect) inside = !inside;
            }
            return inside;
        }

        function closestPointOnSegment(px, py, ax, ay, bx, by){
            const vx = bx - ax, vy = by - ay;
            const wx = px - ax, wy = py - ay;
            const c1 = vx*wx + vy*wy;
            if (c1 <= 0) return [ax, ay];
            const c2 = vx*vx + vy*vy;
            if (c2 <= c1) return [bx, by];
            const t = c1 / c2;
            return [ax + t*vx, ay + t*vy];
        }

        function adjustToPABorder(lat, lon){
            if (pointInPoly(lat, lon, borderCoordinates)) return { lat, lon, adjusted: false };
            let best = null;
            let bestDist2 = Infinity;
            for (let i=0;i<borderCoordinates.length;i++){
                const a = borderCoordinates[i];
                const b = borderCoordinates[(i+1) % borderCoordinates.length];
                // a and b are [lat, lon]
                const [cx, cy] = closestPointOnSegment(lon, lat, a[1], a[0], b[1], b[0]);
                const dx = cx - lon; const dy = cy - lat;
                const d2 = dx*dx + dy*dy;
                if (d2 < bestDist2){ bestDist2 = d2; best = { lat: cy, lon: cx }; }
            }
            return { lat: best.lat, lon: best.lon, adjusted: true };
        }
        if(!latInput || !lonInput) return;
        let lastMarker = null;
        function clearMarker(){ if(lastMarker) try{ map.removeLayer(lastMarker); }catch(e){} lastMarker = null; }
        [latInput, lonInput].forEach(inp => {
            inp.addEventListener('keydown', async (ev)=>{
                if (ev.key === 'Enter'){
                    ev.preventDefault();
                    let lat = parseFloat(latInput.value || latInput.placeholder);
                    let lon = parseFloat(lonInput.value || lonInput.placeholder);
                    if (Number.isFinite(lat) && Number.isFinite(lon)){
                        const adj = adjustToPABorder(lat, lon);
                        if (adj.adjusted){ lat = adj.lat; lon = adj.lon; }
                        try{ clearMarker(); }catch(e){}
                        lastMarker = L.marker([lat, lon]).addTo(map).bindPopup(`Lat: ${lat.toFixed(4)}<br>Lon: ${lon.toFixed(4)}` + (adj.adjusted? ' (rounded to PA border)':'' )).openPopup();
                        map.setView([lat, lon], 12);
                        await plotXYchartForCoord(lat, lon);
                    }
                } else if (ev.key === 'Escape'){
                    ev.preventDefault();
                    latInput.value = '';
                    lonInput.value = '';
                    clearMarker();
                }
            });
        });

        button.onclick = async function() {
            let lat = parseFloat(latInput.value || latInput.placeholder);
            let lon = parseFloat(lonInput.value || lonInput.placeholder);
            if (Number.isFinite(lat) && Number.isFinite(lon)){
                const adj = adjustToPABorder(lat, lon);
                if (adj.adjusted){ lat = adj.lat; lon = adj.lon; }
                try{ clearMarker(); }catch(e){}
                lastMarker = L.marker([lat, lon]).addTo(map).bindPopup(`Lat: ${lat.toFixed(4)}<br>Lon: ${lon.toFixed(4)}` + (adj.adjusted? ' (rounded to PA border)':'' )).openPopup();
                map.setView([lat, lon], 12);
                await plotXYchartForCoord(lat, lon);
            } else {
    
                yearSums = await sumYears(initialYear, false);
                yearSumsAll = await sumYears(initialYear, true);
                plotXYchart(yearSums);
                plotBARchart(yearSums);
                otherBoxes(yearSums);
            }
        }
    }catch(e){ console.warn('Failed to wire dashboard inputs', e); }
});