gcloud compute instances create instance-1  \
    --project=ancient-ship-376517 \
    --zone=us-west4-b \
    --machine-type=n1-standard-1 \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=416362841268-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --create-disk=auto-delete=yes,boot=yes,device-name=instance-1,image=projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20230114,mode=rw,size=10,type=projects/ancient-ship-376517/zones/us-west4-b/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --reservation-affinity=any
    
gcloud compute instances create instance-2  \
    --project=ancient-ship-376517 \
    --zone=us-west4-b \
    --machine-type=n1-standard-1 \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=416362841268-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --create-disk=auto-delete=yes,boot=yes,device-name=instance-2,image=projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20230114,mode=rw,size=10,type=projects/ancient-ship-376517/zones/us-west4-b/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --reservation-affinity=any
 
gcloud compute instances create instance-3  \
    --project=ancient-ship-376517 \
    --zone=us-west4-b \
    --machine-type=n1-standard-1 \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=416362841268-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --create-disk=auto-delete=yes,boot=yes,device-name=instance-3,image=projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20230114,mode=rw,size=10,type=projects/ancient-ship-376517/zones/us-west4-b/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --reservation-affinity=any
  
gcloud compute instances create instance-4  \
    --project=ancient-ship-376517 \
    --zone=us-west4-b \
    --machine-type=n1-standard-1 \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=416362841268-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --create-disk=auto-delete=yes,boot=yes,device-name=instance-4,image=projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20230114,mode=rw,size=10,type=projects/ancient-ship-376517/zones/us-west4-b/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --reservation-affinity=any         

