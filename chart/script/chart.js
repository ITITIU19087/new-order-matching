document.addEventListener('DOMContentLoaded', async (event) => {
    try {
        const responseSell = await fetch('http://localhost:8888/hazelcast/total?side=SELL');
        const dataSell = await responseSell.json();
        const responseBuy = await fetch('http://localhost:8888/hazelcast/total?side=BUY');
        const dataBuy = await responseBuy.json();

        const dataArraySell = Object.entries(dataSell).map(([key, value]) => ({price: parseFloat(key), orders: value, side: 'SELL'}));
        const dataArrayBuy = Object.entries(dataBuy).map(([key, value]) => ({price: parseFloat(key), orders: value, side: 'BUY'}));

        let dataArray = dataArraySell.concat(dataArrayBuy);
        dataArray.sort((a, b) => a.price - b.price);

        const svg = d3.select("#chart"),
            margin = {top: 20, right: 50, bottom: 30, left: 50},
            width = 500 - margin.left - margin.right,
            height = 300 - margin.top - margin.bottom;

        const x = d3.scaleLinear().range([0, width / 4]);
        const y = d3.scaleBand().range([height, 0]).padding(0.1);

        y.domain(dataArray.map(d => d.price));
        x.domain([0, d3.max(dataArray, d => d.orders)]);

        const g = svg.attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);

        const axisGap = 10;

        g.selectAll(".bar")
            .data(dataArray)
            .enter().append("rect")
            .attr("class", d => `bar-${d.side.toLowerCase()}`)
            .attr("y", d => y(d.price))
            .attr("height", y.bandwidth())
            .attr("x", d => {
                if (d.side === 'BUY') {
                    return (width / 2) - x(d.orders) - axisGap;
                } else {
                    return (width / 2) + axisGap;
                }
            })
            .attr("width", d => x(d.orders));

        // Hide tick labels
        const yAxisBuy = d3.axisLeft(y).tickValues([]);
        const yAxisSell = d3.axisRight(y).tickValues([]);

        g.append("g")
            .attr("transform", `translate(${width / 2 + axisGap}, 0)`)
            .call(yAxisSell);

        g.append("g")
            .attr("transform", `translate(${width / 2 - axisGap}, 0)`)
            .call(yAxisBuy);

        // Manually append text for axis labels
        g.selectAll(".axis-text")
            .data(y.domain())
            .enter().append("text")
            .attr("class", "axis-text")
            .attr("y", d => y(d) + y.bandwidth() / 2)
            .attr("dy", ".35em")
            .attr("x", width / 2)
            .attr("text-anchor", "middle")
            .text(d => d);

        // Y position for annotations
        const buyAnnotationY = 10;
        const sellAnnotationY = buyAnnotationY + 30;  // 30 units below the BUY annotation

        const annotationX = 10;  // X position for both annotations

// Annotation for BUY orders
        g.append("rect")
            .attr("x", annotationX)
            .attr("y", buyAnnotationY)
            .attr("width", 20)
            .attr("height", 20)
            .attr("class", "bar-buy");
        g.append("text")
            .attr("x", annotationX + 30)
            .attr("y", buyAnnotationY + 15)
            .attr("class", "annotation")
            .text("BUY orders");

// Annotation for SELL orders
        g.append("rect")
            .attr("x", annotationX)
            .attr("y", sellAnnotationY)
            .attr("width", 20)
            .attr("height", 20)
            .attr("class", "bar-sell");
        g.append("text")
            .attr("x", annotationX + 30)
            .attr("y", sellAnnotationY + 15)
            .attr("class", "annotation")
            .text("SELL orders");


    } catch (error) {
        console.error("Error fetching data: ", error);
    }
});
