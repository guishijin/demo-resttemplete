package com.alphasta.demoresttemplete;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.*;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
import org.pentaho.di.www.GetStatusServlet;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.web.client.RestTemplate;

import java.io.DataOutputStream;
import java.io.FileOutputStream;

@SpringBootApplication
public class DemoResttempleteApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoResttempleteApplication.class, args);

        System.out.println("restTemplate ----------------start");
        try {
            RestTemplateBuilder restTemplateBuilder = new RestTemplateBuilder();
            RestTemplate restTemplate;
            restTemplate = restTemplateBuilder.basicAuthorization("cluster", "cluster").build();
            String quote = restTemplate.getForObject("http://192.168.16.187:9999/"+ GetStatusServlet.CONTEXT_PATH + "/?xml=Y", String.class);
            System.out.println(quote);
        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("restTemplate ----------------end");

    }
}
