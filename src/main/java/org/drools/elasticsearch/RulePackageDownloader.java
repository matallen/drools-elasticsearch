package org.drools.elasticsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.core.util.DroolsStreamUtils;
import org.drools.decisiontable.InputType;
import org.drools.decisiontable.SpreadsheetCompiler;
import org.drools.definition.KnowledgePackage;
import org.drools.definitions.impl.KnowledgePackageImp;
import org.drools.io.ResourceFactory;
import org.drools.io.impl.UrlResource;
import org.drools.rule.Package;

public class RulePackageDownloader {
  private String basePath;
  private String username;
  private String password;
  private boolean enableBasicAuthentication=true;
  private boolean failoverToEmptyRulePackage=false;
  
  public RulePackageDownloader(String path, String username, String password){
    this.basePath=path; 
    this.username=username;
    this.password=password;
  }
  
  public Collection<KnowledgePackage> download(String packageId, String version) throws IOException, ClassNotFoundException{
    if (basePath.toLowerCase().startsWith("http")){
      return downloadFromGuvnor(packageId, version);
    }else{
      return compileFromDisk(packageId, version);
    }
  }
  
  private Collection<KnowledgePackage> compileFromDisk(String packageId, String version) throws FileNotFoundException, IOException{
    KnowledgeBuilder builder=KnowledgeBuilderFactory.newKnowledgeBuilder();
    File packageFile=new File(basePath, packageId.replaceAll("\\.", File.separator)+File.separator+version);
    for(File file:packageFile.listFiles()){
      String drl=null;
      if (file.getName().toLowerCase().matches(".+drl$")){
        drl=IOUtils.toString(new FileInputStream(file));
      }else if (file.getName().toLowerCase().matches(".+xls$")){
        drl=new SpreadsheetCompiler().compile(new FileInputStream(file), InputType.XLS);
      }
      if (null!=drl) builder.add(ResourceFactory.newByteArrayResource(drl.getBytes()), ResourceType.DRL);
    }
    if (builder.hasErrors())
      throw new RuntimeException(builder.getErrors().toString());
    return builder.getKnowledgePackages();
  }
  
  private Collection<KnowledgePackage> downloadFromGuvnor(String packageId, String version) throws IOException, ClassNotFoundException{
    UrlResource res=(UrlResource)ResourceFactory.newUrlResource(new URL(basePath+packageId+"/"+version));
    if (enableBasicAuthentication){
      res.setBasicAuthentication("enabled");
      res.setUsername(username);
      res.setPassword(password);
    }
    List<KnowledgePackage> result=new ArrayList<KnowledgePackage>();
    try{
      Object binaryPkg=DroolsStreamUtils.streamIn(res.getInputStream());
      if (binaryPkg instanceof Collection<?>) {
          result.addAll((Collection<KnowledgePackage>)binaryPkg);
      }else if (binaryPkg instanceof KnowledgePackage) {
          result.add((KnowledgePackage)binaryPkg);
      }else if (binaryPkg instanceof org.drools.rule.Package) {
          result.add(new KnowledgePackageImp((org.drools.rule.Package)binaryPkg));
      }else if (binaryPkg instanceof org.drools.rule.Package[]) {
          for (org.drools.rule.Package pkg:(org.drools.rule.Package[])binaryPkg) {
              result.add(new KnowledgePackageImp(pkg));
          }
      }
    }catch(IOException e){
      if (failoverToEmptyRulePackage)
        result.add(new KnowledgePackageImp(new Package("EMPTY")));// EMPTY PACKAGE - no rules will fire
      else
        throw e;
    }
    return result;
  }
  
//  private org.drools.rule.Package download(URL url, boolean enableBasicAuthentication, String username, String password) throws IOException, ClassNotFoundException {
//    return new HttpClientImpl().fetchPackage(url, enableBasicAuthentication, username, password);
//  }
  
}
