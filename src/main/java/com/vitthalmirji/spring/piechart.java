package com.vitthalmirji.spring;
//import org.knowm.*;

import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;

import java.awt.*;
import java.util.Iterator;
import java.util.Map;


public class piechart {
    public PieChart piecha(Map mapp, String title, int sectors ){
        PieChart chart = new PieChartBuilder().width(800).height(600).title(title).build();
        Color[] sliceColors = new Color[sectors];
        for (int i=0;i<sectors;i++){
            sliceColors[i] = new Color((int) (Math.random( )*256), (int) (Math.random( )*256), (int)(Math.random( )*256));
        }
        chart.getStyler().setSeriesColors(sliceColors);
        Iterator<Map.Entry<String, Integer>> iter = mapp.entrySet().iterator();
        for(int i =0;i<sectors;i++){
            Map.Entry<String, Integer> entry = iter.next();
            chart.addSeries(entry.getKey(), entry.getValue());
        }

        //new SwingWrapper<PieChart>(chart).displayChart();
        return chart;
    }
}

