package com.vitthalmirji.spring;

import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;

import java.util.ArrayList;

public class barchart {
    public CategoryChart b(ArrayList x, ArrayList y,String title){
        CategoryChart chart = new CategoryChartBuilder().width(2000).height(600).title(title).xAxisTitle("Jobs").yAxisTitle(("Number of each job")).build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);

        chart.addSeries(title,x,y);
       // new SwingWrapper<CategoryChart >(chart).displayChart();
        return chart;

    }
}

