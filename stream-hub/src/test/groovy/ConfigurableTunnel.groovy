@Grab(group='ch.ethz.ganymed', module='ganymed-ssh2', version='262')


import java.util.regex.*;
import java.text.SimpleDateFormat;
import ch.ethz.ssh2.*;
import groovy.util.XmlSlurper;


//==================================================================================
//      Tunnel
//==================================================================================
class SSHTunnel {
    def conn = null;
    def userName = System.getProperty('user.name');
    def userHome = System.getProperty('user.home');
    def kh = null;
    def verifier = null;
    def ci = null;
    def authed = false;
    def sshDir = "$userHome\\.ssh";
    def knownHosts = "$sshDir\\known_hosts";
    def myCert = "$sshDir\\id_dsa";    
    def lpfs = [:];
    
    def connectThrough = null;
    def portTunnels = null;
    
    public SSHTunnel(String connectThrough, Map<String, List<int[]>> portTunnels) {        
        this.connectThrough = connectThrough;
        this.portTunnels = portTunnels;
    }
    
    public SSHTunnel open() {
        conn = new Connection(connectThrough);
        
        kh = new KnownHosts(new File(knownHosts));
        verifier = [
            verifyServerHostKey : { hostname, port, serverHostKeyAlgorithm, serverHostKey ->
                println "Verifying $hostname HostKey [${KnownHosts.createHexFingerprint(serverHostKeyAlgorithm, serverHostKey)}] with [$knownHosts] using algo [$serverHostKeyAlgorithm]";
                return true; //0 == kh.verifyHostkey(hostname, serverHostKeyAlgorithm, serverHostKey);
            }
        ] as ServerHostKeyVerifier;
        ci = conn.connect(verifier, 0, 0);
        println "Connected to $connectThrough";
        authed = conn.authenticateWithPublicKey(userName, new File(myCert).getText().toCharArray() , null);
        if(!authed) {
            throw new Exception("Public Key Auth Failed");
        }
        println "Authenticated.";
        //lpf1 = conn.createLocalPortForwarder(1430, "10.6.202.113", 1430);    
        //10.6.202.113
        portTunnels.each() { host, portPairs ->
            portPairs.each() { portPair ->
                def key = "${host}:${portPair}";
                def lpf = null;
                try {
                    if(portPair.length==1) {
                        lpf = conn.createLocalPortForwarder(portPair[0], host, portPair[0]);   
                    } else {
                        lpf = conn.createLocalPortForwarder(portPair[0], host, portPair[1]);   
                    }            
                } catch (ex) {
                    println "FAILED to establish tunnel for [$key]: $ex";
                    throw ex;
                }
                lpfs.put(key, lpf);
                println "Tunnel ${key} Connected";
            }
        }
        println "Local Port Forwards Created";
    }
    
    public void close() {
        lpfs.each() { k, lpf ->
            try { lpf.close();  println "Portforward ${key} Closed"; } catch (e) {}
        }    
        lpfs.clear();
        if(conn!=null) try { conn.close();  println "Connection Closed"; } catch (e) {}
        conn = null;
        println "Tunnel Closed";
    }
}

pi = {s ->
    return Integer.parseInt(s.toString().trim());
}

sshTunnels = [];
int tc = 0;
sshDir = "${System.getProperty('user.home')}/.ssh";
configXml = new File("${sshDir}/configtunnels.xml").getText();
def tunnels = new XmlSlurper().parseText(configXml);
tunnels.connection.each() { conn ->
    def chost = conn.@connect.toString();
    def portPairs = [];
    def tdefs = [:];
    conn.tunnel.each() { t ->
        port = t.@port;
        lport = t.@lport;
        host = t.@host.toString().isEmpty() ? chost : t.@host.toString().trim();
        if(tdefs.containsKey(host)) {
            portPairs = tdefs.get(host);
        } else {
            portPairs = [];
            tdefs.put(host, portPairs);
        }
        if(lport!=null && !lport.toString().trim().isEmpty()) {
            portPairs.add([pi(lport), pi(port)] as int[]);            
        } else {
            portPairs.add([pi(port)] as int[]);
        }        
        tc++;
    }
    sshTunnels.add(new SSHTunnel(chost.toString(), tdefs));
}
println "Loaded ${sshTunnels.size} Tunnel Connection Definitions with $tc Tunnel Definitions";
addShutdownHook {
    println "Stopping Tunnels...";
    sshTunnels.each() { ssht -> 
        try { ssht.close(); } catch (x) {}
    }
    println "Tunnels Stopped";
}

sshTunnels.parallelStream().forEach(
    {it.open();}
);

// sshTunnels.each() { ssht -> 
//     ssht.open();
// }

try {
    Thread.currentThread().join();
} catch (InterruptedException iex) {
    println "Stopping Tunnels...";
    sshTunnels.each() { ssht -> 
        try { ssht.close(); } catch (x) {}
    }
    println "Tunnels Stopped";
}


// try {
//     println "Opening Tunnel.....";
//     tunnel.open();
//     Thread.sleep(30000);
//     tunnel.close();
//     println "Done";
//     tunnel = null;
// } finally {
//     if(tunnel!=null) {
//         try { tunnel.close(); } catch (x) {}
//     }
// }
